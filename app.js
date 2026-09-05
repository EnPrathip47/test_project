/* ============================================================
   AIR CANDY CONTROL — APPLICATION LOGIC
   Direct HiveMQ Cloud MQTT Over WebSocket Secure (Port 8884)
   ============================================================ */

(function () {
  'use strict';

  // ── Configuration ──
  // [หมายเหตุ]: สามารถแก้ไขค่าที่อยู่ HiveMQ Broker Host, Username และ Password ได้ที่นี่
  // หรือสามารถปรับแก้สดๆ ผ่านหน้าเว็บเมนู "การตั้งค่าการเชื่อมต่อ" (Settings Panel) ได้เช่นกัน
  const CONFIG = {
    mqttHost: "35d4bbdea6454305b3cc211b02309fc1.s1.eu.hivemq.cloud", // <--- แก้ไขที่อยู่ HiveMQ Broker ได้ที่นี่
    mqttWebSocketPort: 8884,
    mqttPath: "/mqtt",
    mqttUsername: "esp32s3_aircontrol", // <--- แก้ไข Username จาก HiveMQ Access Management ได้ที่นี่
    mqttPassword: "project123",          // <--- แก้ไข Password จาก HiveMQ Access Management ได้ที่นี่
    topicControl: "aircon/control",       // หัวข้อ MQTT รับคำสั่งจากเว็บ → ESP32
    topicStatus: "aircon/status",         // หัวข้อ MQTT รับสถานะจาก ESP32 → เว็บ
    topicAvailability: "aircon/availability", // หัวข้อ MQTT Heartbeat Online/Offline
    topicSync: "aircon/sync",             // หัวข้อ MQTT สำหรับ Real-time Cross-Device Sync และ Presence
    reconnectDelay: 3000,
    maxReconnectAttempts: 10,
    demoUpdateInterval: 2000,
  };


  // Generate or retrieve persistent Session Client ID for Real-Time Presence & Sync
  if (!sessionStorage.getItem('aircon_client_id')) {
    sessionStorage.setItem('aircon_client_id', 'usr_' + Math.random().toString(36).substring(2, 8));
  }

  // ── State ──
  const state = {
    clientId: sessionStorage.getItem('aircon_client_id'),
    activeUsers: {},
    presenceTimer: null,
    mqttClient: null,
    connected: false,
    demoMode: false,
    demoTimer: null,
    reconnectTimer: null,
    reconnectAttempts: 0,
    acOn: false,
    targetTemp: 25,
    scheduleMode: 'none', // 'none' | 'auto' | 'manual'
    schedule: {
      enabled: false,
      onDate: '',
      onTime: '',
      offDate: '',
      offTime: '',
    },
    sensors: { temp1: null, temp2: null, temp3: null },
    sensorsUpdatedAt: null,
    indicators: { power: false, running: false, fault: false },
    // MQTT Remote Control (Desired & Actual State)
    acPower: 0,     // 0=OFF, 1=ON
    acMode: 0,      // ฟิกซ์โหมดรีโมทเป็น 0 (AUTO) ไว้ตลอดเวลา
    acFan: 0,       // 0=AUTO, 1=LOW, 2=MED, 3=HIGH
    esp32Online: false,
    plcOnline: false,
    mqttOnline: false,
    lastCommand: '',
    irTransmitting: false,
    irTimer: null,

    // User Pending Modifications (Prevent 5s periodic background status overwrite)
    userModifiedPower: false,
    userModifiedMode: false,
    userModifiedFan: false,
    userModifiedTemp: false,
  };

  const SENSOR_COUNT = 3;
  const SENSOR_RING_R = 52;
  const SENSOR_RING_CIRCUMFERENCE = 2 * Math.PI * SENSOR_RING_R;

  // Format DD/MM/YYYY
  function getTodayDDMMYYYY() {
    const now = new Date();
    const dd = String(now.getDate()).padStart(2, '0');
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const yyyy = now.getFullYear();
    return `${dd}/${mm}/${yyyy}`;
  }

  // Convert any date format (YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, Buddhist Year) to ISO YYYY-MM-DD
  function parseThaiDateToIso(dateStr) {
    if (!dateStr) return '';
    const str = String(dateStr).trim();
    const parts = str.split(/[\/\-.]/);
    if (parts.length === 3) {
      let yyyy, mm, dd;
      if (parts[0].length === 4 || parseInt(parts[0], 10) > 1000) {
        // YYYY-MM-DD format (standard HTML5 date input)
        yyyy = parseInt(parts[0], 10);
        mm = parts[1].padStart(2, '0');
        dd = parts[2].padStart(2, '0');
      } else {
        // DD-MM-YYYY or DD/MM/YYYY format
        dd = parts[0].padStart(2, '0');
        mm = parts[1].padStart(2, '0');
        yyyy = parseInt(parts[2], 10);
      }
      if (yyyy > 2400) yyyy -= 543; // Buddhist Era to CE
      return `${yyyy}-${mm}-${dd}`;
    }
    return str;
  }

  // Format date string to DD/MM/YYYY for Thai UI display
  function formatDisplayDate(dateStr) {
    if (!dateStr) return '';
    const iso = parseThaiDateToIso(dateStr);
    const parts = iso.split('-');
    if (parts.length === 3) {
      return `${parts[2]}/${parts[1]}/${parts[0]}`;
    }
    return dateStr;
  }

  // Helper check if time inputs have value
  function isInputFilled() {
    return Boolean(
      (DOM.onTime?.value || state.schedule.onTime) &&
      (DOM.offTime?.value || state.schedule.offTime)
    );
  }

  // Helper check if schedule is fully configured in state
  function isScheduleSet() {
    if (state.scheduleMode === 'auto') return true;
    return Boolean(
      state.schedule.enabled &&
      (state.schedule.onTime || DOM.onTime?.value) &&
      (state.schedule.offTime || DOM.offTime?.value)
    );
  }

  function getTodayIso() {
    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  }

  function validateDateNotPast(inputEl) {
    if (!inputEl || !inputEl.value) return true;
    const todayIso = getTodayIso();
    const inputIso = parseThaiDateToIso(inputEl.value);
    if (inputIso < todayIso) {
      inputEl.value = todayIso;
      showToast('warning', 'ห้ามเลือกวันที่ย้อนหลัง (ปรับเป็นวันปัจจุบันให้อัตโนมัติ)');
      return false;
    }
    return true;
  }

  // ── DOM Elements ──
  const DOM = {
    // Header
    connectionBadge: document.getElementById('connectionBadge'),
    headerClock: document.getElementById('headerClock'),

    // Sensors
    sensorTemp1: document.getElementById('sensorTemp1'),
    sensorTemp2: document.getElementById('sensorTemp2'),
    sensorTemp3: document.getElementById('sensorTemp3'),
    sensorProgress1: document.getElementById('sensorProgress1'),
    sensorProgress2: document.getElementById('sensorProgress2'),
    sensorProgress3: document.getElementById('sensorProgress3'),
    sensorCard1: document.getElementById('sensorCard1'),
    sensorCard2: document.getElementById('sensorCard2'),
    sensorCard3: document.getElementById('sensorCard3'),
    tempUpdateBadge: document.getElementById('tempUpdateBadge'),

    // Indicators
    lightYellow: document.getElementById('lightYellow'),
    lightGreen: document.getElementById('lightGreen'),
    lightRed: document.getElementById('lightRed'),
    stateYellow: document.getElementById('stateYellow'),
    stateGreen: document.getElementById('stateGreen'),
    stateRed: document.getElementById('stateRed'),
    currentStateBadge: document.getElementById('currentStateBadge'),
    scheduleStatusTag: document.getElementById('scheduleStatusTag'),

    // Controls
    onDate: document.getElementById('onDate'),
    onTime: document.getElementById('onTime'),
    offDate: document.getElementById('offDate'),
    offTime: document.getElementById('offTime'),
    targetTemp: document.getElementById('targetTemp'),
    btnSave: document.getElementById('btnSave'),
    btnStart: document.getElementById('btnStart'),
    btnStop: document.getElementById('btnStop'),
    btnReset: document.getElementById('btnReset'),
    btnSaveHint: document.getElementById('btnSaveHint'),
    btnStartHint: document.getElementById('btnStartHint'),
    btnStopHint: document.getElementById('btnStopHint'),

    // State flow
    flowIdle: document.getElementById('flowIdle'),
    flowReady: document.getElementById('flowReady'),
    flowRunning: document.getElementById('flowRunning'),
    flowStopped: document.getElementById('flowStopped'),

    // Settings
    mqttHostInput: document.getElementById('mqttHostInput'),
    mqttUsernameInput: document.getElementById('mqttUsernameInput'),
    mqttPasswordInput: document.getElementById('mqttPasswordInput'),
    connectBtn: document.getElementById('connectBtn'),
    disconnectBtn: document.getElementById('disconnectBtn'),
    demoBtn: document.getElementById('demoBtn'),

    // Log
    logContainer: document.getElementById('logContainer'),
    clearLogBtn: document.getElementById('clearLogBtn'),

    // Toast
    toastContainer: document.getElementById('toastContainer'),

    // Badges & Online Users
    activeUsersCountText: document.getElementById('activeUsersCountText'),
    activeUsersStatus: document.getElementById('activeUsersStatus'),

    // Temp Buttons
    tempMinusBtn: document.getElementById('tempMinusBtn'),
    tempPlusBtn: document.getElementById('tempPlusBtn'),

    // Background
    bgParticles: document.getElementById('bgParticles'),

    // MQTT Remote Control
    powerBtnOn: document.getElementById('powerBtnOn'),
    powerBtnOff: document.getElementById('powerBtnOff'),
    modeSelect: document.getElementById('modeSelect'),
    fanSelect: document.getElementById('fanSelect'),
    btnSendMqtt: document.getElementById('btnSendMqtt'),
    mqttErrorMsg: document.getElementById('mqttErrorMsg'),
    mqttTempDisplay: document.getElementById('mqttTempDisplay'),
    mqttLastCmd: document.getElementById('mqttLastCmd'),
    esp32Status: document.getElementById('esp32Status'),
    plcStatus: document.getElementById('plcStatus'),
    mqttStatus: document.getElementById('mqttStatus'),
    modbusStatus: document.getElementById('modbusStatus'),

    // None/Auto/Manual Mode Toggle
    modeNoneBtn: document.getElementById('modeNoneBtn'),
    modeAutoBtn: document.getElementById('modeAutoBtn'),
    modeManualBtn: document.getElementById('modeManualBtn'),
    modeToggleBar: document.getElementById('modeToggleBar'),
    modeSlider: document.getElementById('modeSlider'),
    modeInfoBadge: document.getElementById('modeInfoBadge'),
    modeInfoDesc: document.getElementById('modeInfoDesc'),
    onGroupLock: document.getElementById('onGroupLock'),
    offGroupLock: document.getElementById('offGroupLock'),
    scheduleGroupOn: document.getElementById('scheduleGroupOn'),
    scheduleGroupOff: document.getElementById('scheduleGroupOff'),
    onDateField: document.getElementById('onDateField'),
    offDateField: document.getElementById('offDateField'),
  };

  // ── Initialize ──
  function init() {
    setupClock();
    setupParticles();
    initSensors();
    bindEvents();
    loadSettings();

    // Set min date for date pickers to today
    const todayIso = getTodayIso();
    if (DOM.onDate) DOM.onDate.min = todayIso;
    if (DOM.offDate) DOM.offDate.min = todayIso;

    applyScheduleMode(state.scheduleMode);

    // Evaluate Real-time State on startup / refresh:
    if (state.scheduleMode === 'auto') {
      const now = new Date();
      const { start, stop } = getScheduleRange(todayIso, '08:00', todayIso, '17:00');
      if (start && stop && now >= start && now < stop) {
        updateSystemState('running');
      } else {
        updateSystemState('ready');
      }
    } else if (state.scheduleMode === 'manual' && state.schedule.enabled) {
      const now = new Date();
      const onD = state.schedule.onDate || todayIso;
      const offD = state.schedule.offDate || onD;
      const { start, stop } = getScheduleRange(onD, state.schedule.onTime, offD, state.schedule.offTime);
      if (start && stop) {
        if (now >= start && now < stop) {
          updateSystemState('running');
        } else if (now >= stop) {
          updateSystemState('timeout');
        } else {
          updateSystemState('ready');
        }
      } else {
        updateSystemState('ready');
      }
    } else {
      updateSystemState(state.systemState || 'idle');
    }

    state.acMode = 0;
    if (DOM.modeSelect) {
      DOM.modeSelect.value = '0';
      DOM.modeSelect.disabled = true;
    }

    updateMqttStatusUI();
    updateMqttTempDisplay();
    addLog('info', `ระบบพร้อมใช้งาน — ใช้งาน HiveMQ Cloud MQTT over WSS (Port ${CONFIG.mqttWebSocketPort})`);

    // Connect to HiveMQ MQTT Broker
    connectMqttBroker();
  }

  function initSensors() {
    const defaultTemps = [25.0, 28.5, 16.0];
    for (let i = 1; i <= SENSOR_COUNT; i++) {
      const progressEl = DOM[`sensorProgress${i}`];
      if (progressEl) {
        progressEl.style.strokeDasharray = String(SENSOR_RING_CIRCUMFERENCE);
      }
      updateSensor(i, defaultTemps[i - 1]);
    }
    updateTempBadge();
  }

  // ── Date & Time Helper Utilities ──
  function parseScheduleDateTime(dateStr, timeStr) {
    if (!timeStr) return null;
    const now = new Date();
    let isoDate = parseThaiDateToIso(dateStr);
    if (!isoDate) {
      const yyyy = now.getFullYear();
      const mm = String(now.getMonth() + 1).padStart(2, '0');
      const dd = String(now.getDate()).padStart(2, '0');
      isoDate = `${yyyy}-${mm}-${dd}`;
    }
    const dateParts = isoDate.split('-');
    if (dateParts.length !== 3) return null;
    const yyyy = parseInt(dateParts[0], 10);
    const mm = parseInt(dateParts[1], 10);
    const dd = parseInt(dateParts[2], 10);

    const timeParts = timeStr.split(':');
    if (timeParts.length !== 2) return null;
    const hh = parseInt(timeParts[0], 10);
    const min = parseInt(timeParts[1], 10);

    const d = new Date(yyyy, mm - 1, dd, hh, min, 0, 0);
    if (isNaN(d.getTime())) return null;
    return d;
  }

  function getScheduleRange(onDateStr, onTimeStr, offDateStr, offTimeStr) {
    if (!onTimeStr || !offTimeStr) return { start: null, stop: null };
    let start = parseScheduleDateTime(onDateStr, onTimeStr);
    let stop = parseScheduleDateTime(offDateStr, offTimeStr);

    if (!start || !stop) return { start: null, stop: null };

    const onIso = parseThaiDateToIso(onDateStr);
    const offIso = parseThaiDateToIso(offDateStr);

    // In Auto mode: if stop is before or equal to start, schedule overnight (+1 day)
    if (state.scheduleMode === 'auto' && stop <= start && (!offDateStr || onIso === offIso)) {
      stop = new Date(stop.getTime() + 24 * 60 * 60 * 1000);
    }

    // In Auto mode: if stop time has already passed today, schedule for next day
    const now = new Date();
    if (state.scheduleMode === 'auto' && stop <= now) {
      start = new Date(start.getTime() + 24 * 60 * 60 * 1000);
      stop = new Date(stop.getTime() + 24 * 60 * 60 * 1000);
    }

    return { start, stop };
  }

  // ── Clock & Schedule Monitor ──
  function setupClock() {
    function updateClock() {
      const now = new Date();
      if (DOM.headerClock) {
        DOM.headerClock.textContent = now.toLocaleTimeString('th-TH', { hour12: false });
      }
      checkScheduleState(now);
      checkEsp32Watchdog();
    }
    updateClock();
    setInterval(updateClock, 1000);
  }

  // Web-side ESP32 Watchdog: หากไม่ได้รับสัญญาณจาก ESP32 เกิน 5 วินาที ให้ปรับเป็น OFFLINE ทันที
  function checkEsp32Watchdog() {
    if (state.connected && state.esp32Online && state.lastEsp32Heartbeat > 0) {
      const diffMs = Date.now() - state.lastEsp32Heartbeat;
      if (diffMs > 5000) { // เกิน 5 วินาทีไม่มีข้อมูลจาก ESP32 (เท่ากับเวลาที่ Online ส่งข้อมูลมา)
        state.esp32Online = false;
        state.plcOnline = false;
        updateMqttStatusUI();
        addLog('warning', 'ESP32 ขาดการติดต่อ (Watchdog Timeout > 5s) -> ปรับเป็น OFFLINE');
      }
    }
  }

  function checkScheduleState(now) {
    if (!state.schedule.enabled) return;

    const onTimeVal = state.schedule.onTime || DOM.onTime?.value;
    const offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    const onDateVal = state.schedule.onDate || DOM.onDate?.value;
    const offDateVal = state.schedule.offDate || DOM.offDate?.value;

    if (!onTimeVal || !offTimeVal) return;

    const { start, stop } = getScheduleRange(onDateVal, onTimeVal, offDateVal, offTimeVal);
    if (!start || !stop) return;

    // โหมด AUTO: วนลูปอัตโนมัติทุกวัน — เมื่อข้ามไปวันถัดไปหรือเวลารอเริ่มรอบใหม่ ให้กลับสู่สถานะ ready เพื่อรอยิงตอน 08:00
    if (state.scheduleMode === 'auto' && (state.systemState === 'timeout' || state.systemState === 'stopped')) {
      if (now < start) {
        updateSystemState('ready');
      }
    }

    // Case 1: เมื่อถึงเวลาเริ่มทำงาน (START TIME) -> สั่งเปิดเครื่องปรับอากาศและยิง IR 10 ครั้ง
    if (state.systemState === 'ready' || state.systemState === 'idle') {
      if (now >= start && now < stop) {
        updateSystemState('running');
        sendMqttPayload(1, getValidTargetTemp(), state.acMode, state.acFan, 0, 0, 0, 0, 1); // start_btn = 1 (Triggers M5 ON & IR 10x)
        addLog('success', `[Schedule] ถึงเวลาเริ่มทำงาน (${onTimeVal}) -> ส่งคำสั่งเปิดเครื่องปรับอากาศและยิงสัญญาณ IR`);
        showToast('success', `ถึงเวลาเปิดเครื่องปรับอากาศแล้ว (${onTimeVal}) — เริ่มทำงานและยิงสัญญาณ IR`);
      }
    }
    // Case 3: เมื่อครบเวลาทำงาน (TIMEOUT / COMPLETE) -> สั่งปิดเครื่องปรับอากาศและยิง IR 10 ครั้ง
    else if (state.systemState === 'running') {
      if (now >= stop) {
        updateSystemState('timeout');
        sendMqttPayload(0, getValidTargetTemp(), state.acMode, state.acFan, 1); // [ Complete Flag set M500 ]
        addLog('warning', `[Schedule] ครบเวลาเปิดเครื่องปรับอากาศ (${offTimeVal}) -> ส่งคำสั่งปิดเครื่องปรับอากาศและยิงสัญญาณ IR`);
        showToast('warning', `ทำงานครบเวลาแล้ว (${offTimeVal}) — ส่งคำสั่งปิดเครื่องปรับอากาศและยิงสัญญาณ IR`);
      }
    }
  }

  // ── Background Particles ──
  function setupParticles() {
    if (!DOM.bgParticles) return;
    for (let i = 0; i < 30; i++) {
      const particle = document.createElement('div');
      particle.className = 'particle';
      particle.style.left = Math.random() * 100 + '%';
      particle.style.top = Math.random() * 100 + '%';
      particle.style.animationDelay = Math.random() * 6 + 's';
      particle.style.animationDuration = (4 + Math.random() * 4) + 's';
      DOM.bgParticles.appendChild(particle);
    }
  }

  // ── Event Binding ──
  function bindEvents() {
    DOM.btnSave?.addEventListener('click', saveSchedule);
    DOM.btnStart?.addEventListener('click', startAC);
    DOM.btnStop?.addEventListener('click', stopAC);
    DOM.btnReset?.addEventListener('click', resetSystem);
    DOM.connectBtn?.addEventListener('click', connectMqttBroker);
    DOM.disconnectBtn?.addEventListener('click', disconnectMqttBroker);
    DOM.demoBtn?.addEventListener('click', toggleDemo);
    DOM.clearLogBtn?.addEventListener('click', clearLog);

    DOM.onDate?.addEventListener('change', () => {
      validateDateNotPast(DOM.onDate);
      state.schedule.onDate = DOM.onDate.value;
      saveSettings();
      updateScheduleInputsState();
      broadcastUiSync('change_input');
    });
    DOM.offDate?.addEventListener('change', () => {
      validateDateNotPast(DOM.offDate);
      state.schedule.offDate = DOM.offDate.value;
      saveSettings();
      updateScheduleInputsState();
      broadcastUiSync('change_input');
    });
    DOM.onTime?.addEventListener('input', () => {
      state.schedule.onTime = DOM.onTime.value;
      saveSettings();
      updateScheduleInputsState();
      broadcastUiSync('change_input');
    });
    DOM.onTime?.addEventListener('change', () => {
      state.schedule.onTime = DOM.onTime.value;
      validateTimeInterval();
      saveSettings();
      broadcastUiSync('change_input');
    });

    DOM.offTime?.addEventListener('input', () => {
      state.schedule.offTime = DOM.offTime.value;
      saveSettings();
      updateScheduleInputsState();
      broadcastUiSync('change_input');
    });
    DOM.offTime?.addEventListener('change', () => {
      state.schedule.offTime = DOM.offTime.value;
      validateTimeInterval();
      saveSettings();
      broadcastUiSync('change_input');
    });

    DOM.targetTemp?.addEventListener('change', () => {
      state.userModifiedTemp = true;
      const validVal = getValidTargetTemp();
      updateMqttTempDisplay();
      saveSettings();
      broadcastUiSync('change_control');
      if (state.connected) {
        const isAuto = (state.scheduleMode === 'auto');
        const isRunning = (state.systemState === 'running' || state.acOn);
        const power = (isRunning || isAuto) ? 1 : state.acPower;
        sendMqttPayload(power, validVal, state.acMode, state.acFan, 0, 0, 1, 0);
      }
    });
    DOM.targetTemp?.addEventListener('input', () => {
      state.userModifiedTemp = true;
      updateMqttTempDisplay();
      broadcastUiSync('change_control');
    });
    DOM.targetTemp?.addEventListener('blur', () => {
      getValidTargetTemp();
      updateMqttTempDisplay();
    });



    // Temp +/- Controls
    DOM.tempMinusBtn?.addEventListener('click', () => adjustTempStep(-1));
    DOM.tempPlusBtn?.addEventListener('click', () => adjustTempStep(1));

    // Temp Presets Chips
    document.querySelectorAll('.temp-chip').forEach((chip) => {
      chip.addEventListener('click', (e) => {
        if (state.scheduleMode === 'none') {
          showToast('warning', 'โหมด NONE ถูกล็อก — กรุณาเลือกโหมด AUTO หรือ MANUAL');
          return;
        }
        const val = parseFloat(e.currentTarget.getAttribute('data-temp'));
        if (!isNaN(val)) setTargetTemp(val, true);
      });
    });

    DOM.modeSelect?.addEventListener('change', () => {
      DOM.modeSelect.value = '0';
      state.acMode = 0;
      showToast('info', 'โหมดรีโมทถูกฟิกซ์เป็น AUTO ไว้ตลอดเวลา');
    });

    DOM.fanSelect?.addEventListener('change', () => {
      if (state.scheduleMode === 'none') return;
      state.acFan = parseInt(DOM.fanSelect.value, 10);
      state.userModifiedFan = true;
      broadcastUiSync('change_control');
    });

    DOM.btnSendMqtt?.addEventListener('click', sendMqttCommandFromUI);

    // None/Auto/Manual Mode Toggle
    DOM.modeNoneBtn?.addEventListener('click', () => setScheduleMode('none'));
    DOM.modeAutoBtn?.addEventListener('click', () => setScheduleMode('auto'));
    DOM.modeManualBtn?.addEventListener('click', () => setScheduleMode('manual'));
  }



  function adjustTempStep(delta) {
    if (state.scheduleMode === 'none') {
      showToast('warning', 'โหมด NONE ถูกล็อก — กรุณาเลือกโหมด AUTO หรือ MANUAL');
      return;
    }
    if (state.irTransmitting) {
      showToast('warning', '⏳ กำลังยิงสัญญาณ IR (10 รอบ)... กรุณารอให้สัญญาณยิงครบ 10 รอบก่อนเปลี่ยนอุณหภูมิ');
      return;
    }
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      showToast('warning', 'ระบบอยู่ในสถานะ Timeout (ล็อกอยู่) — สามารถกดได้เฉพาะปุ่ม "รีเซท" เท่านั้น');
      return;
    }
    const current = parseFloat(DOM.targetTemp?.value) || 25;
    const nextVal = Math.min(27, Math.max(18, current + delta));
    setTargetTemp(nextVal, true);
  }

  function setTargetTemp(val, isUserAction = false) {
    if (state.scheduleMode === 'none') return;
    if (state.irTransmitting) {
      showToast('warning', '⏳ กำลังยิงสัญญาณ IR (10 รอบ)... กรุณารอให้สัญญาณยิงครบ 10 รอบก่อนเปลี่ยนอุณหภูมิ');
      return;
    }
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      showToast('warning', 'ระบบอยู่ในสถานะ Timeout (ล็อกอยู่) — สามารถกดได้เฉพาะปุ่ม "รีเซท" เท่านั้น');
      return;
    }
    const validVal = Math.min(27, Math.max(18, val));
    if (DOM.targetTemp) DOM.targetTemp.value = validVal;
    state.targetTemp = validVal;

    document.querySelectorAll('.temp-chip').forEach((chip) => {
      const chipVal = parseFloat(chip.getAttribute('data-temp'));
      chip.classList.toggle('temp-chip--active', chipVal === validVal);
    });

    saveSettings();
    updateMqttTempDisplay();

    if (isUserAction) {
      state.userModifiedTemp = true;
      broadcastUiSync('change_control');
      if (state.connected) {
        const isAuto = (state.scheduleMode === 'auto');
        const isRunning = (state.systemState === 'running' || state.acOn);
        const power = (isRunning || isAuto) ? 1 : (state.acPower || 0);
        sendMqttPayload(power, validVal, state.acMode, state.acFan, 0, 0, 1, 0);
        showToast('success', `📡 ส่งค่าอุณหภูมิ ${validVal}°C ไปยัง PLC (D11) แล้ว`);
      } else {
        showToast('info', `ตั้งค่าอุณหภูมิเป็น ${validVal}°C แล้ว (ออฟไลน์)`);
      }
    }
  }

  function updateMqttTempDisplay() {
    if (!DOM.mqttTempDisplay) return;
    if (state.scheduleMode === 'none') {
      DOM.mqttTempDisplay.textContent = '--';
      return;
    }
    const temp = DOM.targetTemp?.value || state.targetTemp;
    DOM.mqttTempDisplay.textContent = temp ? `${temp}°C` : '--';
  }

  // Validate Target Temperature (Must be between 18°C and 27°C)
  function getValidTargetTemp() {
    const rawVal = parseFloat(DOM.targetTemp?.value);
    if (isNaN(rawVal) || rawVal < 18 || rawVal > 27) {
      const valid = 24; // หากผู้ใช้ลืมตั้งอุณหภูมิ ให้เซทเป็น 24°C สำรองไว้ก่อน
      state.targetTemp = valid;
      if (state.scheduleMode !== 'none') {
        if (DOM.targetTemp) DOM.targetTemp.value = valid;
        document.querySelectorAll('.temp-chip').forEach((chip) => {
          const chipVal = parseFloat(chip.getAttribute('data-temp'));
          chip.classList.toggle('temp-chip--active', chipVal === valid);
        });
        updateMqttTempDisplay();
      }
      return valid;
    }
    state.targetTemp = rawVal;
    return rawVal;
  }

  // ตรวจสอบความถูกต้องของเวลาปิด ต้องมากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป
  function validateTimeInterval() {
    if (state.scheduleMode !== 'manual') return true;
    const onTimeVal = DOM.onTime?.value;
    const offTimeVal = DOM.offTime?.value;
    const onDateVal = DOM.onDate?.value || getTodayIso();
    const offDateVal = DOM.offDate?.value || onDateVal;

    if (!onTimeVal || !offTimeVal) return true;

    const { start, stop } = getScheduleRange(onDateVal, onTimeVal, offDateVal, offTimeVal);
    if (start && stop) {
      const diffMs = stop.getTime() - start.getTime();
      if (diffMs < 60 * 1000) {
        showToast('warning', 'เวลาปิดเครื่องปรับอากาศต้องตั้งให้มากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป');
        return false;
      }
    }
    return true;
  }

  function updateScheduleInputsState() {
    if (state.schedule.enabled) {
      // เมื่อ setting time แล้ว ห้ามเปลี่ยนค่าจนกว่าจะกดรีเซท
      return;
    }
    updateControlButtons();
  }

  // ── Load Settings from localStorage ──
  function loadSettings() {
    const savedMqtt = localStorage.getItem('airCandyMqttConfig');
    if (savedMqtt) {
      try {
        const m = JSON.parse(savedMqtt);
        if (m.mqttHost) CONFIG.mqttHost = m.mqttHost;
        if (m.mqttUsername) CONFIG.mqttUsername = m.mqttUsername;
        if (m.mqttPassword) CONFIG.mqttPassword = m.mqttPassword;
      } catch (e) { }
    }
    if (DOM.mqttHostInput) DOM.mqttHostInput.value = CONFIG.mqttHost;
    if (DOM.mqttUsernameInput) DOM.mqttUsernameInput.value = CONFIG.mqttUsername;
    if (DOM.mqttPasswordInput) DOM.mqttPasswordInput.value = CONFIG.mqttPassword;

    const saved = localStorage.getItem('airCandySettings') || localStorage.getItem('processAirSettings');
    if (saved) {
      try {
        const settings = JSON.parse(saved);
        if (settings.scheduleMode) {
          state.scheduleMode = settings.scheduleMode;
        }
        if (settings.targetTemp != null) {
          const temp = parseFloat(settings.targetTemp);
          if (!isNaN(temp) && temp >= 18 && temp <= 27) {
            state.targetTemp = temp;
          }
        }
        if (settings.acMode != null) state.acMode = 0; // ฟิกซ์โหมดรีโมทเป็น 0 (AUTO) เสมอ
        if (settings.acFan != null) state.acFan = Number(settings.acFan);
        if (settings.acPower != null) state.acPower = Number(settings.acPower);
        if (settings.acOn != null) state.acOn = Boolean(settings.acOn);

        if (state.scheduleMode === 'manual') {
          state.schedule.onDate = settings.onDate || '';
          state.schedule.offDate = settings.offDate || '';
          state.schedule.onTime = settings.onTime || '';
          state.schedule.offTime = settings.offTime || '';
          state.schedule.enabled = Boolean(settings.scheduleEnabled);
          if (settings.systemState) {
            state.systemState = settings.systemState;
          }
        } else if (state.scheduleMode === 'auto') {
          const todayIso = getTodayIso();
          state.schedule.onDate = todayIso;
          state.schedule.offDate = todayIso;
          state.schedule.onTime = '08:00';
          state.schedule.offTime = '17:00';
          state.schedule.enabled = true;
          if (settings.systemState) {
            state.systemState = settings.systemState;
          }
        } else {
          // In NONE mode:
          state.schedule.enabled = false;
          state.schedule.onDate = '';
          state.schedule.offDate = '';
          state.schedule.onTime = '';
          state.schedule.offTime = '';
          state.systemState = 'idle';
        }
      } catch (e) {
        console.warn('Failed to load settings:', e);
      }
    }
  }

  function saveSettings() {
    try {
      const settings = {
        targetTemp: state.targetTemp,
        scheduleMode: state.scheduleMode,
        scheduleEnabled: state.schedule.enabled,
        onDate: state.schedule.onDate || DOM.onDate?.value || '',
        onTime: state.schedule.onTime || DOM.onTime?.value || '',
        offDate: state.schedule.offDate || DOM.offDate?.value || '',
        offTime: state.schedule.offTime || DOM.offTime?.value || '',
        systemState: state.systemState,
        acOn: state.acOn,
        acPower: state.acPower,
        acMode: state.acMode,
        acFan: state.acFan
      };
      localStorage.setItem('airCandySettings', JSON.stringify(settings));
    } catch (e) {
      console.warn('Failed to save settings:', e);
    }
  }

  // ============================================================
  //  AUTO / MANUAL SCHEDULE MODE
  // ============================================================

  function setScheduleMode(mode) {
    // เมื่อเลือกโหมด AUTO หรือ MANUAL แล้ว จะไม่สามารถเปลี่ยนโหมดได้จนกว่าจะกดปุ่ม "รีเซท" เพื่อกลับไปโหมด NONE
    if (state.scheduleMode !== 'none' && mode !== state.scheduleMode) {
      showToast('warning', '🔒 ล็อคโหมดการทำงานแล้ว — ต้องกดปุ่ม "รีเซท" เพื่อกลับไปโหมด NONE ก่อนเลือกโหมดใหม่');
      addLog('warning', `[Mode] ไม่สามารถสลับเป็นโหมด ${mode.toUpperCase()} ได้ — ต้องกดปุ่มรีเซทเพื่อกลับไปโหมด NONE ก่อน`);
      return;
    }
    if (state.scheduleMode === mode) {
      return;
    }

    state.scheduleMode = mode;
    applyScheduleMode(mode);
    saveSettings();
    broadcastUiSync('change_mode', { scheduleMode: mode });

    const modeNoneFlag = (mode === 'none') ? 1 : 0;
    const modeAutoFlag = (mode === 'auto') ? 1 : 0;
    const modeManualFlag = (mode === 'manual') ? 1 : 0;

    // Send mode flag immediately to PLC via MQTT
    sendMqttPayload(state.acOn ? 1 : 0, getValidTargetTemp(), state.acMode, state.acFan, 0, 0, 0, 0, 0, modeAutoFlag, modeManualFlag, false, 0, modeNoneFlag);

    if (mode === 'none') {
      showToast('info', '⚡ โหมด NONE — ปลดล็อคและกลับสู่โหมด NONE (พร้อมเลือกโหมดใหม่)');
      addLog('info', '[Mode] เปลี่ยนเป็น NONE MODE (ปลดล็อคแล้ว)');
    } else if (mode === 'auto') {
      showToast('info', '🔄 เลือกโหมด AUTO สำเร็จ (เวลาฟิกซ์ 08:00 - 17:00) — ล็อคโหมดแล้ว (กดรีเซทเมื่อต้องการเปลี่ยน)');
      addLog('info', '[Mode] เลือกโหมด AUTO (ล็อคโหมดแล้ว — ต้องกดรีเซทเพื่อกลับไปโหมด NONE)');
    } else if (mode === 'manual') {
      showToast('info', '🛠️ เลือกโหมด MANUAL สำเร็จ (ตั้งเวลาแล้วกด "บันทึกค่า") — ล็อคโหมดแล้ว (กดรีเซทเมื่อต้องการเปลี่ยน)');
      addLog('info', '[Mode] เลือกโหมด MANUAL (ล็อคโหมดแล้ว — ต้องกดรีเซทเพื่อกลับไปโหมด NONE)');
    }
  }

  function applyScheduleMode(mode) {
    const isNone = (mode === 'none');
    const isAuto = (mode === 'auto');
    const isManual = (mode === 'manual');
    const toggle = DOM.modeToggleBar?.querySelector('.mode-toggle');

    // Toggle button active states and locked tooltip titles
    if (DOM.modeNoneBtn) {
      DOM.modeNoneBtn.classList.toggle('mode-toggle__btn--active', isNone);
      DOM.modeNoneBtn.title = isNone ? 'โหมด NONE: ไม่ตั้งเวลา' : 'ต้องกดปุ่ม "รีเซท" เพื่อกลับไปโหมด NONE';
    }
    if (DOM.modeAutoBtn) {
      DOM.modeAutoBtn.classList.toggle('mode-toggle__btn--active', isAuto);
      DOM.modeAutoBtn.title = isAuto ? 'โหมด AUTO กำลังทำงาน' : (isNone ? 'AUTO: ทำงานทุกวัน 08:00-17:00' : 'ล็อคโหมดแล้ว — กดรีเซทเพื่อกลับไปโหมด NONE ก่อน');
    }
    if (DOM.modeManualBtn) {
      DOM.modeManualBtn.classList.toggle('mode-toggle__btn--active', isManual);
      DOM.modeManualBtn.title = isManual ? 'โหมด MANUAL กำลังทำงาน' : (isNone ? 'MANUAL: ปรับตั้งเวลาอิสระ' : 'ล็อคโหมดแล้ว — กดรีเซทเพื่อกลับไปโหมด NONE ก่อน');
    }

    // Slider animation
    if (toggle) {
      toggle.classList.remove('mode-toggle--auto', 'mode-toggle--manual');
      if (isAuto) toggle.classList.add('mode-toggle--auto');
      if (isManual) toggle.classList.add('mode-toggle--manual');
    }

    // Mode info badge & description
    if (DOM.modeInfoBadge) {
      if (isNone) {
        DOM.modeInfoBadge.textContent = '⚡ NONE MODE';
        DOM.modeInfoBadge.className = 'mode-info__badge mode-info__badge--none';
      } else if (isAuto) {
        DOM.modeInfoBadge.textContent = '🔄 AUTO MODE';
        DOM.modeInfoBadge.className = 'mode-info__badge mode-info__badge--auto';
      } else {
        DOM.modeInfoBadge.textContent = '🛠️ MANUAL MODE';
        DOM.modeInfoBadge.className = 'mode-info__badge mode-info__badge--manual';
      }
    }
    if (DOM.modeInfoDesc) {
      if (isNone) {
        DOM.modeInfoDesc.textContent = 'โหมด NONE | ไฟสีเหลืองติดค้าง (สลับเป็น AUTO หรือ MANUAL เพื่อเริ่ม)';
      } else if (isAuto) {
        DOM.modeInfoDesc.textContent = 'ทำงานทุกวัน 08:00 - 17:00 | ปรับอุณหภูมิได้ & กดหยุดได้เมื่อถึงเวลาทำงาน';
      } else {
        DOM.modeInfoDesc.textContent = 'ปรับวันที่ เวลา และอุณหภูมิได้อิสระ';
      }
    }

    // Lock overlays: only show in AUTO mode
    if (DOM.onGroupLock) {
      DOM.onGroupLock.classList.toggle('schedule-group__lock--hidden', !isAuto);
    }
    if (DOM.offGroupLock) {
      DOM.offGroupLock.classList.toggle('schedule-group__lock--hidden', !isAuto);
    }

    // Disable/enable date & time inputs
    if (isNone) {
      state.schedule.enabled = false;
      state.schedule.onDate = '';
      state.schedule.offDate = '';
      state.schedule.onTime = '';
      state.schedule.offTime = '';
      state.acOn = false;
      state.acPower = 0;

      // เคลียร์ค่า input และ display ทั้งหมดให้ว่างเปล่า ไม่มีค่าเดิมค้าง
      if (DOM.onDate) { DOM.onDate.value = ''; DOM.onDate.disabled = true; }
      if (DOM.offDate) { DOM.offDate.value = ''; DOM.offDate.disabled = true; }
      if (DOM.onTime) { DOM.onTime.value = ''; DOM.onTime.disabled = true; }
      if (DOM.offTime) { DOM.offTime.value = ''; DOM.offTime.disabled = true; }
      if (DOM.targetTemp) { DOM.targetTemp.value = ''; DOM.targetTemp.disabled = true; DOM.targetTemp.placeholder = '--'; }
      if (DOM.mqttTempDisplay) DOM.mqttTempDisplay.textContent = '--';
      if (DOM.mqttLastCmd) DOM.mqttLastCmd.textContent = '--';
      document.querySelectorAll('.temp-chip').forEach(chip => {
        chip.classList.remove('temp-chip--active');
        chip.disabled = true;
      });
      if (DOM.powerBtnOn) { DOM.powerBtnOn.classList.remove('mqtt-power-btn--active'); DOM.powerBtnOn.disabled = true; }
      if (DOM.powerBtnOff) { DOM.powerBtnOff.classList.remove('mqtt-power-btn--active'); DOM.powerBtnOff.disabled = true; }

      if (DOM.scheduleStatusTag) {
        DOM.scheduleStatusTag.textContent = '⚡ โหมด NONE: กรุณาสลับโหมดการทำงาน (AUTO / MANUAL)';
        DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--pending';
      }
      if (state.systemState !== 'stopped' && state.systemState !== 'timeout' && state.systemState !== 'running') {
        updateSystemState('idle');
      }
    } else if (isAuto) {
      if (DOM.onTime) { DOM.onTime.value = '08:00'; DOM.onTime.disabled = true; }
      if (DOM.offTime) { DOM.offTime.value = '17:00'; DOM.offTime.disabled = true; }
      const todayIso = getTodayIso();
      if (DOM.onDate) { DOM.onDate.value = todayIso; DOM.onDate.disabled = true; }
      if (DOM.offDate) { DOM.offDate.value = todayIso; DOM.offDate.disabled = true; }
      if (DOM.targetTemp) {
        DOM.targetTemp.disabled = false;
        DOM.targetTemp.value = state.targetTemp ? String(state.targetTemp) : '24';
        DOM.targetTemp.placeholder = '18 - 27';
      }
      updateMqttTempDisplay();

      state.schedule.onDate = todayIso;
      state.schedule.offDate = todayIso;
      state.schedule.onTime = '08:00';
      state.schedule.offTime = '17:00';
      state.schedule.enabled = true;

      const now = new Date();
      const { start, stop } = getScheduleRange(todayIso, '08:00', todayIso, '17:00');
      if (start && stop && now >= start && now < stop) {
        updateSystemState('running');
        sendMqttPayload(1, getValidTargetTemp(), state.acMode, state.acFan);
      } else {
        updateSystemState('ready');
      }
    } else {
      // In manual mode:
      if (!state.schedule.enabled) {
        if (DOM.onDate) {
          DOM.onDate.disabled = false;
          DOM.onDate.value = state.schedule.onDate || '';
        }
        if (DOM.offDate) {
          DOM.offDate.disabled = false;
          DOM.offDate.value = state.schedule.offDate || '';
        }
        if (DOM.onTime) {
          DOM.onTime.disabled = false;
          DOM.onTime.value = state.schedule.onTime || '';
        }
        if (DOM.offTime) {
          DOM.offTime.disabled = false;
          DOM.offTime.value = state.schedule.offTime || '';
        }
        if (DOM.targetTemp) {
          DOM.targetTemp.disabled = false;
          DOM.targetTemp.value = state.targetTemp ? String(state.targetTemp) : '25';
          DOM.targetTemp.placeholder = '18 - 27';
        }
        updateMqttTempDisplay();
        if (state.systemState !== 'stopped' && state.systemState !== 'timeout' && state.systemState !== 'running') {
          updateSystemState('idle');
        }
      } else {
        // If schedule already enabled (setting time แล้ว), restore input values and lock inputs!
        if (DOM.onDate) {
          DOM.onDate.value = state.schedule.onDate || '';
          DOM.onDate.disabled = true;
        }
        if (DOM.offDate) {
          DOM.offDate.value = state.schedule.offDate || '';
          DOM.offDate.disabled = true;
        }
        if (DOM.onTime) {
          DOM.onTime.value = state.schedule.onTime || '';
          DOM.onTime.disabled = true;
        }
        if (DOM.offTime) {
          DOM.offTime.value = state.schedule.offTime || '';
          DOM.offTime.disabled = true;
        }
        if (DOM.targetTemp) {
          DOM.targetTemp.disabled = false;
          DOM.targetTemp.value = state.targetTemp ? String(state.targetTemp) : '25';
          DOM.targetTemp.placeholder = '18 - 27';
        }
        updateMqttTempDisplay();

        const now = new Date();
        const todayIso = getTodayIso();
        const onD = state.schedule.onDate || todayIso;
        const offD = state.schedule.offDate || onD;
        const { start, stop } = getScheduleRange(onD, state.schedule.onTime, offD, state.schedule.offTime);
        if (start && stop) {
          if (now >= start && now < stop) {
            updateSystemState('running');
          } else if (now >= stop) {
            updateSystemState('timeout');
          } else {
            updateSystemState('ready');
          }
        } else {
          updateSystemState('ready');
        }
      }
    }

    // Refresh action buttons based on selected mode
    updateControlButtons();
  }

  // ============================================================
  //  HIVEMQ CLOUD MQTT OVER WEBSOCKET (WSS PORT 8884)
  // ============================================================

  // ── Real-Time Cross-Device Sync & Presence Tracker ──
  function sendPresenceHeartbeat() {
    if (!state.mqttClient || !state.mqttClient.connected) return;
    try {
      const presencePayload = {
        type: 'presence',
        clientId: state.clientId,
        timestamp: Date.now()
      };
      state.mqttClient.publish(CONFIG.topicSync, JSON.stringify(presencePayload));
    } catch (e) { }
  }

  function startPresenceTimer() {
    if (state.presenceTimer) clearInterval(state.presenceTimer);
    state.activeUsers[state.clientId] = Date.now();
    updateActiveUsersCount();
    sendPresenceHeartbeat();
    state.presenceTimer = setInterval(() => {
      sendPresenceHeartbeat();
      pruneInactiveUsers();
    }, 4000);
  }

  function stopPresenceTimer() {
    if (state.presenceTimer) {
      clearInterval(state.presenceTimer);
      state.presenceTimer = null;
    }
  }

  function pruneInactiveUsers() {
    const now = Date.now();
    let changed = false;
    Object.keys(state.activeUsers).forEach(id => {
      if (now - state.activeUsers[id] > 12000 && id !== state.clientId) {
        delete state.activeUsers[id];
        changed = true;
      }
    });
    state.activeUsers[state.clientId] = now;
    if (changed || true) {
      updateActiveUsersCount();
    }
  }

  function updateActiveUsersCount() {
    const count = Object.keys(state.activeUsers).length;
    if (DOM.activeUsersCountText) {
      DOM.activeUsersCountText.textContent = `👥 ออนไลน์: ${count} คน`;
    }
  }

  function broadcastUiSync(actionType, extraData = {}) {
    if (!state.mqttClient || !state.mqttClient.connected) return;
    const syncPayload = {
      type: 'ui_sync',
      senderId: state.clientId,
      action: actionType,
      scheduleMode: state.scheduleMode,
      schedule: { ...state.schedule },
      targetTemp: getValidTargetTemp(),
      acMode: state.acMode,
      acFan: state.acFan,
      systemState: state.systemState,
      onDate: DOM.onDate?.value || state.schedule.onDate || '',
      offDate: DOM.offDate?.value || state.schedule.offDate || '',
      onTime: DOM.onTime?.value || state.schedule.onTime || '',
      offTime: DOM.offTime?.value || state.schedule.offTime || '',
      ...extraData
    };
    try {
      state.mqttClient.publish(CONFIG.topicSync, JSON.stringify(syncPayload));
    } catch (e) { }
  }

  function handleUiSyncMessage(data) {
    if (!data || data.senderId === state.clientId) return; // Ignore own messages

    if (data.type === 'request_sync') {
      broadcastUiSync('full_sync');
      return;
    }

    // Reset user pending modification flags when synced from another user
    state.userModifiedPower = false;
    state.userModifiedMode = false;
    state.userModifiedFan = false;
    state.userModifiedTemp = false;

    // Handle Reset action explicitly
    if (data.action === 'reset_system') {
      state.acOn = false;
      state.schedule.enabled = false;
      state.schedule.onTime = '';
      state.schedule.offTime = '';
      state.schedule.onDate = '';
      state.schedule.offDate = '';

      if (DOM.onTime) DOM.onTime.value = '';
      if (DOM.offTime) DOM.offTime.value = '';
      if (DOM.onDate) DOM.onDate.value = '';
      if (DOM.offDate) DOM.offDate.value = '';

      state.scheduleMode = 'none';
      state.systemState = 'idle';
      applyScheduleMode('none');
      saveSettings();
      showToast('info', '👥 ผู้ใช้อื่นได้ทำการกด "รีเซทระบบ"');
      addLog('info', '[Sync] ผู้ใช้อื่นกดรีเซทระบบ -> สลับเข้าสู่ NONE MODE');
      return;
    }

    // 1. Update Schedule Enabled State FIRST
    if (data.schedule) {
      state.schedule.enabled = Boolean(data.schedule.enabled);
    }

    // 2. Update Date & Time Inputs
    if (data.onDate !== undefined) {
      state.schedule.onDate = data.onDate;
      if (DOM.onDate && document.activeElement !== DOM.onDate) DOM.onDate.value = data.onDate;
    }
    if (data.offDate !== undefined) {
      state.schedule.offDate = data.offDate;
      if (DOM.offDate && document.activeElement !== DOM.offDate) DOM.offDate.value = data.offDate;
    }
    if (data.onTime !== undefined) {
      state.schedule.onTime = data.onTime;
      if (DOM.onTime && document.activeElement !== DOM.onTime) DOM.onTime.value = data.onTime;
    }
    if (data.offTime !== undefined) {
      state.schedule.offTime = data.offTime;
      if (DOM.offTime && document.activeElement !== DOM.offTime) DOM.offTime.value = data.offTime;
    }

    // 3. Update Schedule Mode
    if (data.scheduleMode && data.scheduleMode !== state.scheduleMode) {
      state.scheduleMode = data.scheduleMode;
      applyScheduleMode(data.scheduleMode);
      if (data.action === 'change_mode') {
        const modeText = data.scheduleMode.toUpperCase();
        showToast('info', `👥 ผู้ใช้อื่นสลับระบบเป็นโหมด ${modeText}`);
        addLog('info', `[Sync] ผู้ใช้อื่นสลับระบบเป็นโหมด ${modeText}`);
      }
    } else {
      applyScheduleMode(state.scheduleMode);
    }

    // 4. Update Target Temp, Mode, Fan
    if (data.targetTemp !== undefined) {
      const t = parseFloat(data.targetTemp);
      if (!isNaN(t) && t >= 18 && t <= 27) {
        state.targetTemp = t;
        if (DOM.targetTemp && document.activeElement !== DOM.targetTemp) DOM.targetTemp.value = t;
        updateMqttTempDisplay();
        document.querySelectorAll('.temp-chip').forEach((chip) => {
          const chipVal = parseFloat(chip.getAttribute('data-temp'));
          chip.classList.toggle('temp-chip--active', chipVal === t);
        });
      }
    }
    if (data.acMode !== undefined) {
      state.acMode = parseInt(data.acMode, 10);
      if (DOM.modeSelect && document.activeElement !== DOM.modeSelect) DOM.modeSelect.value = state.acMode;
    }
    if (data.acFan !== undefined) {
      state.acFan = parseInt(data.acFan, 10);
      if (DOM.fanSelect && document.activeElement !== DOM.fanSelect) DOM.fanSelect.value = state.acFan;
    }

    // 5. Update System State
    if (data.systemState && data.systemState !== state.systemState) {
      updateSystemState(data.systemState);
    }

    // Toast notifications for user actions
    if (data.action === 'save_schedule') {
      showToast('success', '👥 ผู้ใช้อื่นได้บันทึกเวลาล่วงหน้าแล้ว');
      addLog('info', '[Sync] ผู้ใช้อื่นกดบันทึกเวลาล่วงหน้า');
    } else if (data.action === 'start_ac') {
      showToast('success', '👥 ผู้ใช้อื่นกดเริ่มทำงาน (START)');
      addLog('info', '[Sync] ผู้ใช้อื่นกดเริ่มทำงานเครื่องปรับอากาศ (START)');
    } else if (data.action === 'stop_ac') {
      showToast('warning', '👥 ผู้ใช้อื่นกดหยุดทำงาน (STOP)');
      addLog('info', '[Sync] ผู้ใช้อื่นกดหยุดทำงานเครื่องปรับอากาศ (STOP)');
    }

    saveSettings();
    updateControlButtons();
  }

  function connectMqttBroker() {
    if (state.demoMode) stopDemo();
    if (state.mqttClient) {
      try { state.mqttClient.end(true); } catch (e) { }
      state.mqttClient = null;
    }

    const rawHost = (DOM.mqttHostInput?.value || CONFIG.mqttHost || '').trim();
    const host = rawHost.replace(/^wss?:\/\//i, '').replace(/\/.*$/, '').split(':')[0];
    const username = (DOM.mqttUsernameInput?.value || CONFIG.mqttUsername || '').trim();
    const password = (DOM.mqttPasswordInput?.value || CONFIG.mqttPassword || '').trim();

    if (!host || !username || !password) {
      showToast('error', 'กรุณากรอกข้อมูล Host, Username และ Password สำหรับ HiveMQ ให้ครบถ้วน');
      addLog('error', 'ไม่สามารถเชื่อมต่อได้ — กรอกข้อมูล Username/Password ไม่ครบ');
      return;
    }

    CONFIG.mqttHost = host;
    CONFIG.mqttUsername = username;
    CONFIG.mqttPassword = password;

    try {
      localStorage.setItem('airCandyMqttConfig', JSON.stringify({
        mqttHost: host,
        mqttUsername: username,
        mqttPassword: password,
      }));
    } catch (e) { }

    const brokerUrl = `wss://${host}:${CONFIG.mqttWebSocketPort}${CONFIG.mqttPath}`;
    addLog('info', `กำลังเชื่อมต่อ HiveMQ Cloud (${host}:${CONFIG.mqttWebSocketPort}) [User: ${username}]...`);

    const clientId = 'WebDashboard-' + Math.random().toString(16).substring(2, 10);

    if (typeof mqtt === 'undefined') {
      addLog('info', 'กำลังดึงไลบรารี MQTT.js จาก CDN...');
      const script = document.createElement('script');
      script.src = 'https://cdnjs.cloudflare.com/ajax/libs/mqtt/5.10.2/mqtt.min.js';
      script.onload = () => {
        addLog('success', 'โหลดไลบรารี MQTT.js สำเร็จ! กำลังเริ่มเชื่อมต่อ MQTT...');
        connectMqttBroker();
      };
      script.onerror = () => {
        addLog('error', 'ไม่สามารถโหลดไลบรารี MQTT.js ได้');
        showToast('error', 'ไม่พบไลบรารี MQTT.js (กรุณาเช็คอินเทอร์เน็ต)');
      };
      document.head.appendChild(script);
      return;
    }

    try {
      state.mqttClient = mqtt.connect(brokerUrl, {
        clientId: clientId,
        username: username,
        password: password,
        clean: true,
        keepalive: 60,
        reconnectPeriod: CONFIG.reconnectDelay,
        connectTimeout: 15000,
        resubscribe: true,
      });

      state.mqttClient.on('connect', () => {
        state.connected = true;
        state.mqttOnline = true;
        updateConnectionUI('connected');
        updateMqttStatusUI();

        if (DOM.connectBtn) DOM.connectBtn.disabled = true;
        if (DOM.disconnectBtn) DOM.disconnectBtn.disabled = false;

        state.mqttClient.subscribe(CONFIG.topicStatus);
        state.mqttClient.subscribe(CONFIG.topicAvailability);
        state.mqttClient.subscribe(CONFIG.topicControl);
        state.mqttClient.subscribe(CONFIG.topicSync, () => {
          try {
            state.mqttClient.publish(CONFIG.topicSync, JSON.stringify({ type: 'request_sync', senderId: state.clientId }));
          } catch (e) { }
        });

        startPresenceTimer();

        addLog('success', 'เชื่อมต่อ HiveMQ Cloud MQTT Over WSS สำเร็จ!');
        showToast('success', 'เชื่อมต่อ HiveMQ MQTT สำเร็จ');
      });

      state.mqttClient.on('message', (topic, payload) => {
        try {
          const msgStr = payload.toString().trim();
          if (topic === CONFIG.topicAvailability) {
            const isOnline = (msgStr.toLowerCase() === 'online');
            state.esp32Online = isOnline;
            if (isOnline) {
              state.lastEsp32Heartbeat = Date.now();
            } else {
              state.plcOnline = false;
            }
            updateMqttStatusUI();
            addLog('info', `ESP32 Status: ${isOnline ? 'ONLINE' : 'OFFLINE'}`);
          } else if (topic === CONFIG.topicStatus) {
            state.lastEsp32Heartbeat = Date.now();
            state.esp32Online = true;
            const statusData = JSON.parse(msgStr);
            handleMqttStatus(statusData);
          } else if (topic === CONFIG.topicSync || topic === CONFIG.topicControl) {
            try {
              const syncData = JSON.parse(msgStr);
              if (syncData && typeof syncData === 'object') {
                if (syncData.type === 'presence') {
                  state.activeUsers[syncData.clientId] = Date.now();
                  updateActiveUsersCount();
                } else if (syncData.type === 'ui_sync' || syncData.type === 'request_sync') {
                  handleUiSyncMessage(syncData);
                }
              }
            } catch (e) { }
          }
        } catch (err) {
          console.warn('Invalid MQTT Message:', topic, payload.toString());
        }
      });

      state.mqttClient.on('close', () => {
        state.connected = false;
        state.mqttOnline = false;
        state.esp32Online = false;
        state.plcOnline = false;
        updateConnectionUI('disconnected');
        updateMqttStatusUI();
        if (DOM.connectBtn) DOM.connectBtn.disabled = false;
        if (DOM.disconnectBtn) DOM.disconnectBtn.disabled = true;
      });

      state.mqttClient.on('offline', () => {
        state.connected = false;
        state.mqttOnline = false;
        state.esp32Online = false;
        state.plcOnline = false;
        updateConnectionUI('disconnected');
        updateMqttStatusUI();
      });

      state.mqttClient.on('error', (err) => {
        state.connected = false;
        state.mqttOnline = false;
        state.esp32Online = false;
        state.plcOnline = false;
        updateConnectionUI('disconnected');
        updateMqttStatusUI();
        addLog('error', `MQTT Connection Error: ${err.message || 'ไม่สามารถเชื่อมต่อ HiveMQ Cloud ได้'}`);
      });
    } catch (err) {
      addLog('error', `MQTT Exception: ${err.message}`);
    }
  }

  function disconnectMqttBroker() {
    if (state.mqttClient) {
      try { state.mqttClient.end(true); } catch (e) { }
      state.mqttClient = null;
    }
    state.connected = false;
    state.mqttOnline = false;
    state.esp32Online = false;
    state.plcOnline = false;
    updateConnectionUI('disconnected');
    updateMqttStatusUI();
    if (DOM.connectBtn) DOM.connectBtn.disabled = false;
    if (DOM.disconnectBtn) DOM.disconnectBtn.disabled = true;
    addLog('info', 'ตัดการเชื่อมต่อ MQTT');
    showToast('info', 'ตัดการเชื่อมต่อแล้ว');
  }

  // Send Direct JSON MQTT Command to HiveMQ (aircon/control)
  function sendMqttPayload(power, temp, mode, fan, complete = 0, reset = 0, mqttSend = 0, stopBtn = 0, startBtn = 0, modeAuto = 0, modeManual = 0, includeStopDate = true, saveBtn = 0, modeNone = 0) {
    // Number type validation
    const p = Number(power);
    const t = Number(temp);
    const m = 0; // ฟิกซ์โหมดรีโมทเป็น 0 (AUTO) ไว้ตลอดเวลา
    const f = Number(fan);
    const c = Number(complete) || 0;
    const r = Number(reset) || 0;
    const ms = Number(mqttSend) || 0;
    const sv = Number(saveBtn) || 0;
    const sb = Number(stopBtn) || 0;
    const tb = Number(startBtn) || 0;
    const mn = (state.scheduleMode === 'none' || modeNone) ? 1 : 0;
    const ma = (state.scheduleMode === 'auto' || modeAuto) ? 1 : 0;
    const mm = (state.scheduleMode === 'manual' || modeManual) ? 1 : 0;

    const validationErrors = validatePayloadValues(p, t, m, f);
    if (validationErrors.length > 0) {
      if (DOM.mqttErrorMsg) DOM.mqttErrorMsg.textContent = validationErrors.join(', ');
      showToast('error', validationErrors[0]);
      return false;
    }
    if (DOM.mqttErrorMsg) DOM.mqttErrorMsg.textContent = '';

    const now = new Date();

    // Calculate Start Date (D500-D504) and Stop Date (D600-D604)
    let startDt = now;
    let stopDt = null;

    const onTimeVal = state.schedule.onTime || DOM.onTime?.value;
    const offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    const onDateVal = state.schedule.onDate || DOM.onDate?.value;
    const offDateVal = state.schedule.offDate || DOM.offDate?.value;

    if (onTimeVal && offTimeVal) {
      const { start, stop } = getScheduleRange(onDateVal, onTimeVal, offDateVal, offTimeVal);
      if (start) startDt = start;
      if (stop) stopDt = stop;
    }

    // In Auto mode OR when includeStopDate is false: DO NOT send Stop Date (D500-D504)!
    const sendStopDate = includeStopDate && (state.scheduleMode !== 'auto');

    const payloadObj = {
      power: p,
      temperature: t,
      mode: m,
      fan: f,
      complete: c,
      reset: r,
      mqtt_send: ms,
      save_btn: sv,
      stop_btn: sb,
      start_btn: tb,
      mode_none: mn,
      mode_auto: ma,
      mode_manual: mm,
      schedule_mode: state.scheduleMode,

      // D500 - D504: Start DateTime
      start_year: startDt.getFullYear(),
      start_month: startDt.getMonth() + 1,
      start_day: startDt.getDate(),
      start_hour: startDt.getHours(),
      start_minute: startDt.getMinutes(),

      // Legacy fallback
      year: startDt.getFullYear(),
      month: startDt.getMonth() + 1,
      day: startDt.getDate(),
      hour: startDt.getHours(),
      minute: startDt.getMinutes(),

      // D600 - D604: Stop DateTime
      stop_year: stopDt ? stopDt.getFullYear() : 0,
      stop_month: stopDt ? stopDt.getMonth() + 1 : 0,
      stop_day: stopDt ? stopDt.getDate() : 0,
      stop_hour: stopDt ? stopDt.getHours() : 0,
      stop_minute: stopDt ? stopDt.getMinutes() : 0
    };
    const payloadStr = JSON.stringify(payloadObj);

    const modeNames = ['AUTO', 'COOL', 'DRY', 'FAN'];
    const fanNames = ['AUTO', 'LOW', 'MEDIUM', 'HIGH'];
    const summary = `Power=${p ? 'ON' : 'OFF'} Temp=${t}°C Mode=${modeNames[m]} Fan=${fanNames[f]}`;

    // Reset user pending modification flags after sending command
    state.userModifiedPower = false;
    state.userModifiedMode = false;
    state.userModifiedFan = false;
    state.userModifiedTemp = false;

    if (state.mqttClient && state.mqttClient.connected) {
      state.mqttClient.publish(CONFIG.topicControl, payloadStr);
      addLog('success', `MQTT Command Sent: ${summary}`);
      showToast('success', `ส่งคำสั่งไปยัง ESP32-S3 สำเร็จ — ${summary}`);
      if (DOM.mqttLastCmd) DOM.mqttLastCmd.textContent = summary;
      return true;
    } else {
      showToast('warning', 'ยังไม่ได้เชื่อมต่อ HiveMQ Cloud MQTT');
      addLog('warning', 'ไม่สามารถส่งคำสั่งได้ — ยังไม่ได้เชื่อมต่อ MQTT Broker');
      return false;
    }
  }

  function validatePayloadValues(power, temp, mode, fan) {
    const errors = [];
    if (power !== 0 && power !== 1) {
      errors.push('ค่า Power ต้องเป็น 0 หรือ 1 เท่านั้น');
    }
    if (isNaN(temp) || temp < 18 || temp > 27) {
      errors.push('ค่าอุณหภูมิต้องอยู่ระหว่าง 18°C ถึง 27°C เท่านั้น');
    }
    if (isNaN(mode) || mode < 0 || mode > 3) {
      errors.push('ค่า Mode ต้องอยู่ระหว่าง 0 ถึง 3 เท่านั้น');
    }
    if (isNaN(fan) || fan < 0 || fan > 3) {
      errors.push('ค่า Fan Speed ต้องอยู่ระหว่าง 0 ถึง 3 เท่านั้น');
    }
    return errors;
  }

  function startIrTransmissionLock(durationMs = 5500) {
    state.irTransmitting = true;
    updateTempControlButtonsLock();

    if (state.irTimer) clearTimeout(state.irTimer);
    state.irTimer = setTimeout(() => {
      state.irTransmitting = false;
      state.irTimer = null;
      updateTempControlButtonsLock();
      showToast('info', '✅ ยิงสัญญาณ IR ครบ 10 รอบแล้ว — สามารถปรับอุณหภูมิใหม่ได้');
    }, durationMs);
  }

  function updateTempControlButtonsLock() {
    const isLocked = (state.scheduleMode === 'none' || state.irTransmitting || state.systemState === 'stopped' || state.systemState === 'timeout');

    if (DOM.tempMinusBtn) DOM.tempMinusBtn.disabled = isLocked;
    if (DOM.tempPlusBtn) DOM.tempPlusBtn.disabled = isLocked;
    if (DOM.targetTemp) DOM.targetTemp.disabled = isLocked;
    if (DOM.btnSendMqtt) DOM.btnSendMqtt.disabled = isLocked;

    document.querySelectorAll('.temp-chip').forEach(chip => {
      chip.disabled = isLocked;
    });
  }

  function isCurrentlyInWorkingWindow() {
    if (state.scheduleMode === 'none') return false;

    let onTimeVal = state.schedule.onTime || DOM.onTime?.value;
    let offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    let onDateVal = state.schedule.onDate || DOM.onDate?.value;
    let offDateVal = state.schedule.offDate || DOM.offDate?.value;

    if (state.scheduleMode === 'auto') {
      onTimeVal = '08:00';
      offTimeVal = '17:00';
      const todayIso = getTodayIso();
      onDateVal = todayIso;
      offDateVal = todayIso;
    }

    if (!onTimeVal || !offTimeVal) return false;

    const { start, stop } = getScheduleRange(onDateVal, onTimeVal, offDateVal, offTimeVal);
    if (!start || !stop) return false;

    const now = new Date();
    return (now >= start && now < stop);
  }

  function sendMqttCommandFromUI() {
    if (state.scheduleMode === 'none') {
      showToast('warning', 'กดปุ่ม SEND MQTT ได้เฉพาะโหมด AUTO หรือ MANUAL เท่านั้น');
      return;
    }
    if (state.irTransmitting) {
      showToast('warning', '⏳ กำลังยิงสัญญาณ IR (10 รอบ)... กรุณารอให้สัญญาณยิงครบ 10 รอบก่อนส่งคำสั่งใหม่');
      return;
    }

    const temp = getValidTargetTemp();
    const mode = state.acMode;
    const fan = state.acFan;
    const isAuto = (state.scheduleMode === 'auto');
    const isRunning = (state.systemState === 'running' || state.acOn);
    const power = (isRunning || isAuto) ? 1 : (state.acPower || 1);

    // กดปุ่ม SEND MQTT (M8) -> ส่ง mqtt_send = 1 (พัลส์ขอบขาขึ้น) ไปหา ESP32 เพื่อพัลส์ M8 และเขียน D11 ลง PLC
    const success = sendMqttPayload(power, temp, mode, fan, 0, 0, 1, 0);
    if (success) {
      startIrTransmissionLock(5500);
      showToast('success', `📡 [SEND MQTT] ส่งพัลส์ M8 และค่าอุณหภูมิ ${temp}°C ไปยัง PLC (D11) สำเร็จ`);
      addLog('success', `[SEND MQTT] ส่งพัลส์ M8 และอุณหภูมิ ${temp}°C เขียนลง PLC (D11) ทันที (ยิง IR 10 รอบ)`);

    }
  }

  // Show HMI Notification Popup Modal / Banner
  function showHmiCommandPopup(title, details, temp, startTimeStr, stopTimeStr) {
    if (!document.getElementById('hmiPopupStyles')) {
      const style = document.createElement('style');
      style.id = 'hmiPopupStyles';
      style.textContent = `
        @keyframes hmiSlideIn {
          from { opacity: 0; transform: translateY(-20px) scale(0.95); }
          to { opacity: 1; transform: translateY(0) scale(1); }
        }
      `;
      document.head.appendChild(style);
    }

    const existing = document.getElementById('hmiNotificationModal');
    if (existing) existing.remove();

    const modal = document.createElement('div');
    modal.id = 'hmiNotificationModal';
    modal.style.cssText = `
      position: fixed;
      top: 24px;
      right: 24px;
      z-index: 99999;
      max-width: 440px;
      background: rgba(15, 23, 42, 0.95);
      border: 1.5px solid rgba(56, 189, 248, 0.6);
      border-radius: 16px;
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.7), 0 0 24px rgba(56, 189, 248, 0.25);
      backdrop-filter: blur(16px);
      color: #f8fafc;
      padding: 18px 22px;
      font-family: 'Inter', system-ui, sans-serif;
      animation: hmiSlideIn 0.35s cubic-bezier(0.16, 1, 0.3, 1);
    `;

    let timeDetailsHtml = '';
    if (startTimeStr || stopTimeStr) {
      timeDetailsHtml = `
        <div style="margin-top: 12px; padding: 10px 14px; background: rgba(30, 41, 59, 0.8); border-radius: 10px; font-size: 0.88rem; line-height: 1.6; border: 1px solid rgba(255, 255, 255, 0.08);">
          ${startTimeStr ? `<div>⏱️ <strong>เวลาเริ่ม (+2 นาที):</strong> <span style="color: #4ade80; font-weight: bold;">${startTimeStr}</span></div>` : ''}
          ${stopTimeStr ? `<div>⏰ <strong>เวลาปิด (HMI ตั้งไว้):</strong> <span style="color: #f87171; font-weight: bold;">${stopTimeStr}</span></div>` : ''}
        </div>
      `;
    }

    modal.innerHTML = `
      <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 12px;">
        <div style="display: flex; align-items: center; gap: 12px;">
          <div style="width: 40px; height: 40px; border-radius: 12px; background: linear-gradient(135deg, #0284c7, #38bdf8); display: flex; align-items: center; justify-content: center; font-size: 1.3rem; flex-shrink: 0; box-shadow: 0 4px 12px rgba(56, 189, 248, 0.3);">
            🎛️
          </div>
          <div>
            <div style="font-weight: 700; font-size: 1.05rem; color: #38bdf8;">คำสั่งจากจอ HMI / PLC</div>
            <div style="font-size: 0.84rem; color: #cbd5e1; margin-top: 2px;">${details}</div>
          </div>
        </div>
        <button id="closeHmiModalBtn" style="background: none; border: none; color: #94a3b8; font-size: 1.4rem; cursor: pointer; padding: 0 4px; line-height: 1;">&times;</button>
      </div>
      <div style="margin-top: 14px; display: flex; align-items: center; justify-content: space-between; font-size: 0.92rem; background: rgba(56, 189, 248, 0.08); padding: 8px 12px; border-radius: 8px;">
        <span style="color: #cbd5e1;">🌡️ อุณหภูมิเป้าหมายที่ HMI ตั้งไว้:</span>
        <span style="font-size: 1.15rem; font-weight: 800; color: #38bdf8; background: rgba(56, 189, 248, 0.2); padding: 2px 10px; border-radius: 6px;">${temp}°C</span>
      </div>
      ${timeDetailsHtml}
      <div style="margin-top: 12px; font-size: 0.75rem; color: #64748b; text-align: right;">✓ อัปเดตค่าลงหน้าเว็บเรียบร้อยแล้ว</div>
    `;

    document.body.appendChild(modal);

    const closeBtn = modal.querySelector('#closeHmiModalBtn');
    if (closeBtn) {
      closeBtn.onclick = () => modal.remove();
    }

    setTimeout(() => {
      if (document.body.contains(modal)) {
        modal.style.transition = 'opacity 0.4s ease, transform 0.4s ease';
        modal.style.opacity = '0';
        modal.style.transform = 'translateY(-10px)';
        setTimeout(() => modal.remove(), 400);
      }
    }, 8000);
  }

  // Handle incoming status payload from ESP32 (Actual Real-Time State Readback from PLC/HMI)
  function handleMqttStatus(mqttData) {
    if (!mqttData || typeof mqttData !== 'object') return;

    // 1. Sync Active Mode from PLC/HMI (M9, M100, M101 / schedule_mode)
    if (mqttData.schedule_mode && typeof mqttData.schedule_mode === 'string') {
      const serverMode = mqttData.schedule_mode.toLowerCase();
      if (serverMode === 'none' || serverMode === 'auto' || serverMode === 'manual') {
        if (state.scheduleMode !== serverMode) {
          applyScheduleMode(serverMode);
        }
      }
    }

    // 2. Sync Real Target Temperature (D11) from PLC/HMI
    if (mqttData.temperature !== undefined) {
      const t = parseFloat(mqttData.temperature);
      if (!isNaN(t) && t >= 18 && t <= 27) {
        state.targetTemp = t;
        if (DOM.targetTemp && document.activeElement !== DOM.targetTemp) {
          DOM.targetTemp.value = t;
        }
        updateMqttTempDisplay();
      }
    }

    // 3. Sync AC Power & Control States from PLC Readback
    if (mqttData.power !== undefined) {
      state.acPower = Number(mqttData.power);
      state.acOn = (state.acPower === 1);
      updateControlButtons();
    }

    if (mqttData.mode !== undefined) {
      state.acMode = 0; // ฟิกซ์โหมดรีโมทเป็น 0 (AUTO) เสมอ
      if (DOM.modeSelect) {
        DOM.modeSelect.value = '0';
        DOM.modeSelect.disabled = true;
      }
    }

    if (mqttData.fan !== undefined) {
      state.acFan = Number(mqttData.fan);
      if (DOM.fanSelect && document.activeElement !== DOM.fanSelect) {
        DOM.fanSelect.value = state.acFan;
      }
    }

    // 4. Sync PLC Real-Time Clock (D400-D404 from TRD) to Header Display
    if (mqttData.plc_hour !== undefined && mqttData.plc_minute !== undefined) {
      const h = String(mqttData.plc_hour).padStart(2, '0');
      const m = String(mqttData.plc_minute).padStart(2, '0');
      if (DOM.headerClock) {
        DOM.headerClock.textContent = `${h}:${m} (PLC)`;
      }
    }

    // 5. Sync Stop Time from PLC (D500-D504) in Manual Mode
    if (mqttData.stop_hour !== undefined && mqttData.stop_minute !== undefined) {
      if (Number(mqttData.stop_hour) > 0 || Number(mqttData.stop_minute) > 0) {
        const sh = String(mqttData.stop_hour).padStart(2, '0');
        const sm = String(mqttData.stop_minute).padStart(2, '0');
        const sy = mqttData.stop_year || (new Date()).getFullYear();
        const smo = String(mqttData.stop_month || ((new Date()).getMonth() + 1)).padStart(2, '0');
        const sd = String(mqttData.stop_day || (new Date()).getDate()).padStart(2, '0');
        const stopTimeStr = `${sh}:${sm}`;
        const stopDateStr = `${sy}-${smo}-${sd}`;

        if (state.scheduleMode === 'manual') {
          state.schedule.offTime = stopTimeStr;
          state.schedule.offDate = stopDateStr;
          if (DOM.offTime && document.activeElement !== DOM.offTime) DOM.offTime.value = stopTimeStr;
          if (DOM.offDate && document.activeElement !== DOM.offDate) DOM.offDate.value = stopDateStr;
        }
      }
    }

    // 6. Process HMI Command Notification & Real-Time Sync
    if (mqttData.hmi_cmd && typeof mqttData.hmi_cmd === 'string' && mqttData.hmi_cmd.length > 0) {
      const cmd = mqttData.hmi_cmd;
      const curTemp = (mqttData.temperature !== undefined) ? parseFloat(mqttData.temperature) : state.targetTemp;

      let startStr = '';
      let stopStr = '';

      const isCurrentAuto = (state.scheduleMode === 'auto' || cmd === 'temp_auto' || mqttData.mode_auto === 1);

      if (isCurrentAuto) {
        // โหมด AUTO: เวลาฟิกซ์ไว้แล้ว (08:00 - 17:00 น.) เปลี่ยนได้แค่อุณหภูมิเท่านั้น
        const todayIso = getTodayIso();
        startStr = '08:00 (ฟิกซ์เวลา)';
        stopStr = '17:00 (ฟิกซ์เวลา)';

        state.schedule.onTime = '08:00';
        state.schedule.offTime = '17:00';
        state.schedule.onDate = todayIso;
        state.schedule.offDate = todayIso;
        state.schedule.enabled = true;

        if (DOM.onTime) { DOM.onTime.value = '08:00'; DOM.onTime.disabled = true; }
        if (DOM.offTime) { DOM.offTime.value = '17:00'; DOM.offTime.disabled = true; }
        if (DOM.onDate) { DOM.onDate.value = todayIso; DOM.onDate.disabled = true; }
        if (DOM.offDate) { DOM.offDate.value = todayIso; DOM.offDate.disabled = true; }
      } else {
        // โหมด MANUAL: อัปเดตเวลาเริ่ม (คำนวณ +2 นาที) และเวลาปิดตามที่ HMI ตั้งไว้ (D500-D504)
        if (mqttData.plc_hour !== undefined && mqttData.plc_minute !== undefined) {
          let pHour = Number(mqttData.plc_hour);
          let pMin = Number(mqttData.plc_minute) + 2;
          let pYear = Number(mqttData.plc_year || (new Date()).getFullYear());
          let pMonth = Number(mqttData.plc_month || ((new Date()).getMonth() + 1));
          let pDay = Number(mqttData.plc_day || (new Date()).getDate());

          if (pMin >= 60) {
            pMin -= 60;
            pHour = (pHour + 1) % 24;
          }

          const calcOnTime = `${String(pHour).padStart(2, '0')}:${String(pMin).padStart(2, '0')}`;
          const calcOnDate = `${pYear}-${String(pMonth).padStart(2, '0')}-${String(pDay).padStart(2, '0')}`;
          startStr = `${calcOnTime} (${formatDisplayDate(calcOnDate)})`;

          state.schedule.onTime = calcOnTime;
          state.schedule.onDate = calcOnDate;
          if (DOM.onTime) DOM.onTime.value = calcOnTime;
          if (DOM.onDate) DOM.onDate.value = calcOnDate;
        }

        if (mqttData.stop_hour !== undefined && mqttData.stop_minute !== undefined && (Number(mqttData.stop_hour) > 0 || Number(mqttData.stop_minute) > 0)) {
          const sHour = Number(mqttData.stop_hour);
          const sMin = Number(mqttData.stop_minute);
          const sYear = Number(mqttData.stop_year || (new Date()).getFullYear());
          const sMonth = Number(mqttData.stop_month || ((new Date()).getMonth() + 1));
          const sDay = Number(mqttData.stop_day || (new Date()).getDate());

          const calcOffTime = `${String(sHour).padStart(2, '0')}:${String(sMin).padStart(2, '0')}`;
          const calcOffDate = `${sYear}-${String(sMonth).padStart(2, '0')}-${String(sDay).padStart(2, '0')}`;
          stopStr = `${calcOffTime} (${formatDisplayDate(calcOffDate)})`;

          state.schedule.offTime = calcOffTime;
          state.schedule.offDate = calcOffDate;
          if (DOM.offTime) DOM.offTime.value = calcOffTime;
          if (DOM.offDate) DOM.offDate.value = calcOffDate;
        }
      }

      // อัปเดตอุณหภูมิเป้าหมาย (เปลี่ยนได้ทั้งในโหมด Auto และ Manual)
      if (!isNaN(curTemp) && curTemp >= 18 && curTemp <= 27) {
        state.targetTemp = curTemp;
        if (DOM.targetTemp) DOM.targetTemp.value = curTemp;
        updateMqttTempDisplay();
      }

      let detailMsg = 'มีการสั่งงานผ่านจอ HMI / PLC';
      if (cmd === 'start_manual') {
        detailMsg = 'โหมด MANUAL (M5+M60+M61 ครบเงื่อนไข): เริ่มทำงานทันที';
        state.acOn = true;
        state.schedule.enabled = true;
        updateSystemState('running');
      } else if (cmd === 'temp_auto') {
        detailMsg = `โหมด AUTO (M70+M71 ครบเงื่อนไข): ปรับอุณหภูมิใหม่เป็น ${curTemp}°C (เวลาฟิกซ์ 08:00 - 17:00 น.)`;
        updateMqttTempDisplay();
      } else if (cmd === 'temp_change') {
        detailMsg = 'ปรับค่าอุณหภูมิแอร์เป้าหมาย (D11)';
      } else if (cmd === 'mode_change') {
        detailMsg = `สลับโหมดการทำงาน (${(mqttData.schedule_mode || state.scheduleMode).toUpperCase()})`;
      } else if (cmd === 'timeout_stop') {
        detailMsg = 'ครบเวลาทำงานตามที่ HMI ตั้งไว้ (Timeout Stop)';
      }

      showHmiCommandPopup('คำสั่งจากจอ HMI / PLC', detailMsg, curTemp, startStr, stopStr);
      addLog('info', `🎛️ [HMI Command] ${detailMsg} — อุณหภูมิ: ${curTemp}°C ${startStr ? '| เวลาเริ่ม (+2 นาที): ' + startStr : ''} ${stopStr ? '| เวลาปิด: ' + stopStr : ''}`);
    }

    // 7. Hardware Status Badges
    if (mqttData.esp32_online !== undefined) {
      state.esp32Online = Boolean(mqttData.esp32_online);
    }
    if (mqttData.plc_online !== undefined) {
      state.plcOnline = Boolean(mqttData.plc_online);
    }

    // 8. 3 Temperature Sensors Readback (D1-D3)
    if (mqttData.temp1 !== undefined) updateSensor(1, parseFloat(mqttData.temp1));
    if (mqttData.temp2 !== undefined) updateSensor(2, parseFloat(mqttData.temp2));
    if (mqttData.temp3 !== undefined) updateSensor(3, parseFloat(mqttData.temp3));
    if (mqttData.temp1 !== undefined || mqttData.temp2 !== undefined || mqttData.temp3 !== undefined) {
      updateTempBadge();
    }

    // 9. Sync Real Hardware Lamps (Y0-Y2 / M1-M3) & Timeout / Complete Flag (M500)
    const isM500Complete = Boolean(mqttData.complete === 1 || mqttData.complete === true || mqttData.m500 === 1 || mqttData.m500_complete === 1);
    const isRedOn = Boolean(mqttData.m2_red === 1 || mqttData.y0_red === 1);
    const isGreenOn = Boolean(mqttData.m1_green === 1 || mqttData.y2_green === 1);
    const isYellowOn = Boolean(mqttData.m3_yellow === 1 || mqttData.y1_yellow === 1);

    if (isM500Complete || (isRedOn && state.systemState !== 'stopped')) {
      if (isM500Complete && state.systemState !== 'timeout') {
        state.acOn = false;
        state.acPower = 0;
        updateSystemState('timeout');
        addLog('warning', '⏰ PLC/HMI แจ้งเตือน: ครบเวลาทำงาน (M500 Complete) — แอร์หยุดทำงานแล้ว (กรุณากดปุ่มรีเซทเพื่อเริ่มใหม่)');
        showToast('warning', '⏰ ครบเวลาทำงานแล้ว (Timeout) — กรุณากดปุ่มรีเซท');
      } else if (!isM500Complete && isRedOn && state.systemState !== 'stopped' && state.systemState !== 'timeout') {
        state.acOn = false;
        state.acPower = 0;
        updateSystemState('stopped');
      }
    } else if (isGreenOn && state.systemState !== 'running') {
      state.acOn = true;
      state.acPower = 1;
      updateSystemState('running');
    } else if (isYellowOn && state.systemState !== 'idle' && state.systemState !== 'ready' && state.systemState !== 'running') {
      updateSystemState('idle');
    }

    updateMqttStatusUI();
  }

  function updateMqttStatusUI() {
    if (DOM.esp32Status) {
      DOM.esp32Status.className = 'mqtt-status-badge ' +
        (state.esp32Online ? 'mqtt-status-badge--online' : 'mqtt-status-badge--offline');
      DOM.esp32Status.innerHTML =
        '<span class="mqtt-status-badge__dot"></span>' +
        'ESP32: ' + (state.esp32Online ? 'ONLINE' : 'OFFLINE');
    }
    if (DOM.plcStatus) {
      DOM.plcStatus.className = 'mqtt-status-badge ' +
        (state.plcOnline ? 'mqtt-status-badge--online' : 'mqtt-status-badge--offline');
      DOM.plcStatus.innerHTML =
        '<span class="mqtt-status-badge__dot"></span>' +
        'PLC: ' + (state.plcOnline ? 'ONLINE' : 'OFFLINE');
    }
    if (DOM.mqttStatus) {
      DOM.mqttStatus.className = 'mqtt-status-badge ' +
        (state.mqttOnline ? 'mqtt-status-badge--online' : 'mqtt-status-badge--offline');
      DOM.mqttStatus.innerHTML =
        '<span class="mqtt-status-badge__dot"></span>' +
        'MQTT: ' + (state.mqttOnline ? 'ONLINE' : 'OFFLINE');
    }
    if (DOM.modbusStatus) {
      const modbusActive = state.esp32Online && state.plcOnline;
      DOM.modbusStatus.className = 'mqtt-status-badge ' +
        (modbusActive ? 'mqtt-status-badge--online' : 'mqtt-status-badge--offline');
      DOM.modbusStatus.innerHTML =
        '<span class="mqtt-status-badge__dot"></span>' +
        'MODBUS: ' + (modbusActive ? 'ONLINE' : 'OFFLINE');
    }
  }

  // ============================================================
  //  UI UPDATES
  // ============================================================

  function updateConnectionUI(status) {
    const badge = DOM.connectionBadge;
    if (!badge) return;
    badge.className = 'connection-badge';
    const textEl = badge.querySelector('.connection-badge__text');

    switch (status) {
      case 'connected':
        badge.classList.add('connection-badge--connected');
        if (textEl) textEl.textContent = 'เชื่อมต่อแล้ว';
        break;
      case 'demo':
        badge.classList.add('connection-badge--demo');
        if (textEl) textEl.textContent = 'โหมดจำลอง';
        break;
      default:
        if (textEl) textEl.textContent = 'ไม่ได้เชื่อมต่อ';
    }
  }

  // ── Sensors ──
  function updateSensor(index, value) {
    if (index < 1 || index > SENSOR_COUNT) return;

    const tempEl = DOM[`sensorTemp${index}`];
    const progressEl = DOM[`sensorProgress${index}`];
    const cardEl = DOM[`sensorCard${index}`];
    if (!tempEl || !progressEl || !cardEl) return;

    if (value == null || isNaN(value)) {
      if (state.sensors[`temp${index}`] != null) return;
      value = (index === 1) ? 25.0 : (index === 2) ? 28.5 : 16.0;
    }

    const temp = parseFloat(value);
    state.sensors[`temp${index}`] = temp;

    tempEl.textContent = temp.toFixed(1);

    const pct = Math.min(Math.max(temp / 50, 0), 1);
    progressEl.style.strokeDashoffset = String(SENSOR_RING_CIRCUMFERENCE * (1 - pct));

    cardEl.classList.remove('sensor-card--cool', 'sensor-card--warm', 'sensor-card--hot', 'sensor-card--offline');
    if (temp < 24) {
      cardEl.classList.add('sensor-card--cool');
    } else if (temp < 30) {
      cardEl.classList.add('sensor-card--warm');
    } else {
      cardEl.classList.add('sensor-card--hot');
    }
  }

  function updateTempBadge() {
    if (!DOM.tempUpdateBadge) return;
    const now = new Date();
    state.sensorsUpdatedAt = now;
    const time = now.toLocaleTimeString('th-TH', { hour12: false });
    DOM.tempUpdateBadge.textContent = `${time} อัปเดตล่าสุด`;
  }

  // ── Status Indicators (3 lights) ──
  function setLight(lightEl, stateEl, mode, label, desc) {
    if (!lightEl || !stateEl) return;
    lightEl.className = 'indicator__light';
    if (mode) lightEl.classList.add(`indicator__light--${mode}`);
    else lightEl.classList.add('indicator__light--off');
    if (label) stateEl.textContent = label;
    const descEl = stateEl.parentElement?.querySelector('.indicator__desc');
    if (descEl && desc) descEl.textContent = desc;
  }

  function updateSystemState(nextState) {
    state.systemState = nextState;

    setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ');
    setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ');
    setLight(DOM.lightRed, DOM.stateRed, null, 'OFF', '❌ ดับ');

    [DOM.flowIdle, DOM.flowReady, DOM.flowRunning, DOM.flowStopped].forEach((el) => {
      el?.classList.remove('state-flow__step--active');
    });

    if (DOM.scheduleStatusTag) {
      if (nextState === 'timeout') {
        DOM.scheduleStatusTag.textContent = '⛔ Step 4: ครบเวลาทำงาน (ไฟแดงกระพริบ 1s — ต้องกด "รีเซท" เท่านั้น)';
        DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--pending';
      } else if (nextState === 'stopped') {
        DOM.scheduleStatusTag.textContent = '⛔ Case 2: กดหยุดทำงาน (ไฟแดงติดค้าง — ต้องกด "รีเซท" เท่านั้น)';
        DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--pending';
      } else if (nextState === 'running') {
        const onT = state.schedule.onTime || DOM.onTime?.value || '--:--';
        const offT = state.schedule.offTime || DOM.offTime?.value || '--:--';
        DOM.scheduleStatusTag.textContent = `🟢 กำลังทำงาน (${onT} - ${offT})`;
        DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--active';
      } else if (nextState === 'ready') {
        const onT = state.schedule.onTime || DOM.onTime?.value || '';
        const onD = state.schedule.onDate || DOM.onDate?.value || '';
        const todayIso = getTodayIso();
        const onIso = parseThaiDateToIso(onD);
        let timeText = '';
        if (onT) {
          if (onIso && onIso > todayIso) {
            timeText = ` (รอเปิด: ${formatDisplayDate(onD)} ${onT})`;
          } else {
            timeText = ` (รอถึงเวลา ${onT})`;
          }
        }
        DOM.scheduleStatusTag.textContent = `⚡ ตั้งค่าล่วงหน้าแล้ว — รอถึงเวลาเริ่ม${timeText}`;
        DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--ready';
      } else {
        if (state.scheduleMode === 'none') {
          DOM.scheduleStatusTag.textContent = '⚡ โหมด NONE : กรุณาสลับโหมดการทำงาน (AUTO / MANUAL)';
          DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--pending';
        } else {
          DOM.scheduleStatusTag.textContent = '⚠️ Step 1: IDLE (สแตนด์บาย / รอตั้งเวลา)';
          DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--pending';
        }
      }
    }

    switch (nextState) {
      case 'ready':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ (ตั้งเวลาแล้ว)');
        setLight(DOM.lightGreen, DOM.stateGreen, 'green-blink', 'READY', 'READY: พร้อมทำงาน / รอถึงเวลาเริ่ม');
        DOM.flowReady?.classList.add('state-flow__step--active');
        if (DOM.currentStateBadge) {
          const onT = state.schedule.onTime || DOM.onTime?.value || '';
          const onD = state.schedule.onDate || DOM.onDate?.value || '';
          const todayIso = getTodayIso();
          const onIso = parseThaiDateToIso(onD);
          let timeText = '';
          if (onT) {
            if (onIso && onIso > todayIso) {
              timeText = ` [รอเปิด: ${formatDisplayDate(onD)} ${onT}]`;
            } else {
              timeText = ` (รอถึงเวลา ${onT})`;
            }
          }
          DOM.currentStateBadge.textContent = `Step 2: READY (พร้อมทำงาน)${timeText}`;
          DOM.currentStateBadge.className = 'state-badge state-badge--ready';
        }
        break;

      case 'running':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ');
        setLight(DOM.lightGreen, DOM.stateGreen, 'green-solid', 'RUNNING', '🟢 RUNNING: เครื่องปรับอากาศกำลังทำงาน');
        DOM.flowRunning?.classList.add('state-flow__step--active');
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'Step 3: RUNNING (กำลังทำงาน)';
          DOM.currentStateBadge.className = 'state-badge state-badge--running';
        }
        break;

      case 'timeout':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightRed, DOM.stateRed, 'red-blink', 'TIMEOUT', 'TIMEOUT: ครบเวลาทำงาน (ต้องกดรีเซท)');
        DOM.flowStopped?.classList.add('state-flow__step--active');
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'Step 4: TIMEOUT (ครบเวลาทำงาน - กดรีเซท)';
          DOM.currentStateBadge.className = 'state-badge state-badge--stopped';
        }
        break;

      case 'stopped':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightRed, DOM.stateRed, 'red-solid', 'STOPPED', 'STOPPED: กดหยุดทำงาน (ต้องกดรีเซท)');
        DOM.flowStopped?.classList.add('state-flow__step--active');
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'STOPPED (หยุดทำงาน - กดรีเซท)';
          DOM.currentStateBadge.className = 'state-badge state-badge--stopped';
        }
        break;

      case 'idle':
      default:
        state.systemState = 'idle';
        DOM.flowIdle?.classList.add('state-flow__step--active');
        const idleDot = DOM.flowIdle?.querySelector('.state-flow__dot');

        if (state.scheduleMode === 'none') {
          // ในโหมด NONE
          setLight(DOM.lightYellow, DOM.stateYellow, 'amber-solid', 'NONE', '🟡 โหมด NONE: สแตนด์บาย');
          if (idleDot) idleDot.className = 'state-flow__dot state-flow__dot--amber';
          if (DOM.currentStateBadge) {
            DOM.currentStateBadge.textContent = 'โหมด NONE (สแตนด์บาย)';
            DOM.currentStateBadge.className = 'state-badge state-badge--amber';
          }
        } else {
          // ในโหมด AUTO / MANUAL ที่ยังไม่ได้บันทึกเวลา
          setLight(DOM.lightYellow, DOM.stateYellow, 'amber-blink', 'IDLE', 'IDLE: สแตนด์บาย / รอตั้งเวลา');
          if (idleDot) idleDot.className = 'state-flow__dot state-flow__dot--amber-blink';
          if (DOM.currentStateBadge) {
            DOM.currentStateBadge.textContent = 'Step 1: IDLE (สแตนด์บาย / รอตั้งเวลา)';
            DOM.currentStateBadge.className = 'state-badge state-badge--amber-blink';
          }
        }
        break;
    }

    updateControlButtons();
  }

  function isScheduleSet() {
    const onT = state.schedule.onTime || DOM.onTime?.value;
    const offT = state.schedule.offTime || DOM.offTime?.value;
    return Boolean(onT && offT && (state.schedule.enabled || (state.schedule.onDate && state.schedule.offDate)));
  }

  function updateControlButtons() {
    const isNone = (state.scheduleMode === 'none');
    const isAuto = (state.scheduleMode === 'auto');
    const isManual = (state.scheduleMode === 'manual');

    // ตรวจสอบความถูกต้องของการกรอกเวลาในโหมด MANUAL
    const onTimeVal = DOM.onTime?.value || state.schedule.onTime;
    const offTimeVal = DOM.offTime?.value || state.schedule.offTime;
    const onDateVal = DOM.onDate?.value || state.schedule.onDate;
    const offDateVal = DOM.offDate?.value || state.schedule.offDate;
    const hasValidTimes = Boolean(onTimeVal && offTimeVal && onDateVal && offDateVal);

    // ──────────────────────────────────────────────────────────────
    // 1. สถานะ STOPPED หรือ TIMEOUT (ระบบล็อก ต้องกดรีเซทเท่านั้น)
    // ──────────────────────────────────────────────────────────────
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      if (DOM.btnSave) {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = '';
      }
      if (DOM.btnStart) {
        DOM.btnStart.disabled = true;
        if (DOM.btnStartHint) DOM.btnStartHint.textContent = 'ระบบถูกล็อก (กดรีเซท)';
      }
      if (DOM.btnStop) {
        DOM.btnStop.disabled = true;
      }
      if (DOM.btnReset) {
        DOM.btnReset.disabled = false;
        if (DOM.btnResetHint) DOM.btnResetHint.textContent = 'กดรีเซทเพื่อกลับไปโหมด NONE';
      }

      // Lock schedule inputs during TIMEOUT / STOPPED state
      if (DOM.onDate) DOM.onDate.disabled = true;
      if (DOM.offDate) DOM.offDate.disabled = true;
      if (DOM.onTime) DOM.onTime.disabled = true;
      if (DOM.offTime) DOM.offTime.disabled = true;

      // ปลดล็อกปุ่มรีเซท
      if (DOM.btnReset) {
        DOM.btnReset.disabled = false;
        if (DOM.btnResetHint) DOM.btnResetHint.textContent = 'กดรีเซทเพื่อกลับไปโหมด NONE';
      }

      // ปลดล็อกปุ่มสลับโหมดเพื่อให้เลือกโหมดใหม่ได้
      if (DOM.modeNoneBtn) DOM.modeNoneBtn.disabled = false;
      if (DOM.modeAutoBtn) DOM.modeAutoBtn.disabled = false;
      if (DOM.modeManualBtn) DOM.modeManualBtn.disabled = false;
      return;
    }

    // ──────────────────────────────────────────────────────────────
    // 2. โหมด NONE: ล็อกปุ่มเวลาและปุ่ม SEND MQTT (กดส่งได้เฉพาะ AUTO หรือ MANUAL)
    // ──────────────────────────────────────────────────────────────
    if (isNone) {
      if (DOM.onDate) DOM.onDate.disabled = true;
      if (DOM.offDate) DOM.offDate.disabled = true;
      if (DOM.onTime) DOM.onTime.disabled = true;
      if (DOM.offTime) DOM.offTime.disabled = true;

      // เปิดให้เลือกอุณหภูมิเป้าหมายได้
      if (DOM.targetTemp) DOM.targetTemp.disabled = false;
      if (DOM.tempMinusBtn) DOM.tempMinusBtn.disabled = false;
      if (DOM.tempPlusBtn) DOM.tempPlusBtn.disabled = false;
      if (DOM.modeSelect) { DOM.modeSelect.value = "0"; DOM.modeSelect.disabled = true; }
      if (DOM.fanSelect) DOM.fanSelect.disabled = false;
      document.querySelectorAll('.temp-chip').forEach(chip => chip.disabled = false);

      // ล็อกปุ่ม SEND MQTT ในโหมด NONE (กดได้เฉพาะ 2 โหมดคือ AUTO หรือ MANUAL)
      if (DOM.btnSendMqtt) {
        DOM.btnSendMqtt.disabled = true;
        DOM.btnSendMqtt.title = 'กดปุ่ม SEND MQTT ได้เฉพาะในโหมด AUTO หรือ MANUAL เท่านั้น';
      }

      if (DOM.btnSave) {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = 'กรุณาสลับโหมดเพื่อเริ่มใช้งาน';
        DOM.btnSave.title = 'โหมด NONE ไม่สามารถใช้งานได้';
      }
      if (DOM.btnStart) {
        DOM.btnStart.disabled = true;
        if (DOM.btnStartHint) DOM.btnStartHint.textContent = 'กรุณาสลับโหมดเพื่อเริ่มใช้งาน';
        DOM.btnStart.title = 'โหมด NONE ไม่สามารถใช้งานได้';
      }
      if (DOM.btnStop) {
        DOM.btnStop.disabled = true;
        DOM.btnStop.title = 'โหมด NONE ไม่สามารถใช้งานได้';
      }
      if (DOM.btnReset) {
        DOM.btnReset.disabled = false;
      }

      // ปลดล็อกปุ่มสลับโหมด (NONE / AUTO / MANUAL)
      if (DOM.modeNoneBtn) DOM.modeNoneBtn.disabled = false;
      if (DOM.modeAutoBtn) DOM.modeAutoBtn.disabled = false;
      if (DOM.modeManualBtn) DOM.modeManualBtn.disabled = false;
      return;
    }

    // ──────────────────────────────────────────────────────────────
    // 3. โหมด AUTO: เวลาฟิกซ์ 08:00 - 17:00 (ปรับอุณหภูมิได้, กดส่ง MQTT ได้, ล็อกปุ่ม STOP จนกว่าจะถึงเวลาทำงาน)
    // ──────────────────────────────────────────────────────────────
    if (isAuto) {
      if (DOM.onDate) DOM.onDate.disabled = true;
      if (DOM.offDate) DOM.offDate.disabled = true;
      if (DOM.onTime) DOM.onTime.disabled = true;
      if (DOM.offTime) DOM.offTime.disabled = true;

      // ปลดล็อกให้ปรับอุณหภูมิและกดปุ่ม SEND MQTT ได้ (1 ใน 2 โหมดที่กดได้)
      if (DOM.targetTemp) DOM.targetTemp.disabled = false;
      if (DOM.tempMinusBtn) DOM.tempMinusBtn.disabled = false;
      if (DOM.tempPlusBtn) DOM.tempPlusBtn.disabled = false;
      if (DOM.modeSelect) { DOM.modeSelect.value = "0"; DOM.modeSelect.disabled = true; }
      if (DOM.fanSelect) DOM.fanSelect.disabled = false;
      if (DOM.btnSendMqtt) {
        DOM.btnSendMqtt.disabled = false;
        DOM.btnSendMqtt.title = 'ส่งค่าอุณหภูมิไปยัง PLC (D11)';
      }
      document.querySelectorAll('.temp-chip').forEach(chip => chip.disabled = false);

      // ล็อกปุ่มบันทึกและเริ่มทำงาน (เพราะเวลาฟิกซ์อัตโนมัติแล้ว)
      if (DOM.btnSave) {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = 'โหมด AUTO ฟิกซ์เวลาแล้ว';
        DOM.btnSave.title = 'โหมด AUTO ฟิกซ์เวลาอัตโนมัติ';
      }
      if (DOM.btnStart) {
        DOM.btnStart.disabled = true;
        if (DOM.btnStartHint) DOM.btnStartHint.textContent = (state.systemState === 'running') ? 'กำลังทำงานอัตโนมัติ' : 'ทำงานอัตโนมัติตามเวลา';
        DOM.btnStart.title = 'โหมด AUTO ทำงานอัตโนมัติ';
      }

      // โหมด AUTO: ล็อกปุ่ม STOP ไว้จนกว่าจะถึงเวลาทำงาน (08:00 - 17:00 น. และระบบทำงานอยู่)
      const isAutoRunning = (state.systemState === 'running' || state.acOn) || isCurrentlyInWorkingWindow();
      if (DOM.btnStop) {
        DOM.btnStop.disabled = !isAutoRunning;
        if (DOM.btnStopHint) {
          DOM.btnStopHint.textContent = isAutoRunning ? 'กดเพื่อหยุดการทำงาน' : 'ล็อกปุ่มไว้จนกว่าจะถึงเวลาทำงาน (08:00 - 17:00)';
        }
        DOM.btnStop.title = isAutoRunning ? 'กดเพื่อหยุดการทำงาน (OFF)' : 'ปุ่มล็อกอยู่ จะกดได้เมื่อถึงเวลาทำงาน (08:00 - 17:00)';
      }
      if (DOM.btnReset) {
        DOM.btnReset.disabled = false; // กดได้เฉพาะปุ่มรีเซท!
      }

      // ปลดล็อกปุ่มสลับโหมด
      if (DOM.modeNoneBtn) DOM.modeNoneBtn.disabled = false;
      if (DOM.modeAutoBtn) DOM.modeAutoBtn.disabled = false;
      if (DOM.modeManualBtn) DOM.modeManualBtn.disabled = false;
      return;
    }

    // ──────────────────────────────────────────────────────────────
    // 4. โหมด MANUAL: ปลดล็อกตามลำดับขั้นตอนที่กำหนด
    // ──────────────────────────────────────────────────────────────
    if (DOM.targetTemp) DOM.targetTemp.disabled = false;
    if (DOM.tempMinusBtn) DOM.tempMinusBtn.disabled = false;
    if (DOM.tempPlusBtn) DOM.tempPlusBtn.disabled = false;
    if (DOM.modeSelect) { DOM.modeSelect.value = "0"; DOM.modeSelect.disabled = true; }
    if (DOM.fanSelect) DOM.fanSelect.disabled = false;
    if (DOM.btnSendMqtt) DOM.btnSendMqtt.disabled = false;
    document.querySelectorAll('.temp-chip').forEach(chip => chip.disabled = false);

    if (DOM.modeNoneBtn) DOM.modeNoneBtn.disabled = false;
    if (DOM.modeAutoBtn) DOM.modeAutoBtn.disabled = false;
    if (DOM.modeManualBtn) DOM.modeManualBtn.disabled = false;

    // การล็อกเวลา: เมื่อบันทึกค่าแล้ว (state.schedule.enabled == true) จะล็อกทันทีจนกว่าจะกดรีเซท
    const isTimeLocked = state.schedule.enabled;
    if (DOM.onDate) DOM.onDate.disabled = isTimeLocked;
    if (DOM.offDate) DOM.offDate.disabled = isTimeLocked;
    if (DOM.onTime) DOM.onTime.disabled = isTimeLocked;
    if (DOM.offTime) DOM.offTime.disabled = isTimeLocked;

    // ขั้นที่ 1 & 2: ปุ่มบันทึกค่า (btnSave)
    // - ถ้ายังไม่ตั้งค่าเวลาครบ (hasValidTimes == false): ล็อก (disabled = true)
    // - ถ้าตั้งเวลาเสร็จแล้ว (hasValidTimes == true) และยังไม่ได้บันทึก (!state.schedule.enabled): ปลดล็อกให้กดบันทึกค่าได้!
    // - พอบันทึกค่าเสร็จแล้ว (state.schedule.enabled == true): ล็อก (disabled = true)
    if (DOM.btnSave) {
      if (!hasValidTimes) {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = 'กรุณาตั้งเวลาให้ครบ';
      } else if (!state.schedule.enabled) {
        DOM.btnSave.disabled = false;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = 'กดเพื่อบันทึกค่า';
      } else {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = 'บันทึกเวลาแล้ว';
      }
      DOM.btnSave.title = isTimeLocked ? 'บันทึกเวลาเรียบร้อยแล้ว (กดรีเซทเพื่อเปลี่ยนค่าใหม่)' : (hasValidTimes ? 'กดเพื่อบันทึกค่า' : 'กรุณาตั้งเวลาให้ครบ');
    }

    // ขั้นที่ 3: ปุ่มเริ่มทำงาน (btnStart)
    // - พอบันทึกค่าเสร็จ (state.schedule.enabled == true) และยังไม่ running: ให้กดปุ่มเริ่มทำงานได้!
    // - ถ้ายังไม่บันทึกค่า หรือกำลัง running: ล็อก (disabled = true)
    const canStart = state.schedule.enabled && (state.systemState !== 'running');
    if (DOM.btnStart) {
      DOM.btnStart.disabled = !canStart;
      if (DOM.btnStartHint) {
        if (state.systemState === 'running') {
          DOM.btnStartHint.textContent = 'กำลังทำงาน';
        } else if (state.schedule.enabled) {
          DOM.btnStartHint.textContent = 'กดเพื่อเริ่มทำงาน';
        } else {
          DOM.btnStartHint.textContent = 'ต้องกดบันทึกค่าก่อน';
        }
      }
    }

    // ขั้นที่ 4: ปุ่มหยุดทำงาน (btnStop)
    // - เมื่อเครื่องปรับอากาศถึงเวลาทำงานที่ตั้งไว้ (state.systemState === 'running'): ถึงจะสามารถกดปุ่มหยุดการทำงานได้!
    // - นอกนั้น: ล็อก (disabled = true)
    if (DOM.btnStop) {
      DOM.btnStop.disabled = (state.systemState !== 'running');
    }

    // ปุ่มรีเซท: สามารถกดรีเซทได้เสมอในโหมด MANUAL
    if (DOM.btnReset) {
      DOM.btnReset.disabled = false;
    }
  }

  function setMqttPower(val, isUserAction = false) {
    if (state.scheduleMode === 'none' || state.scheduleMode === 'auto') {
      showToast('warning', state.scheduleMode === 'none' ? 'โหมด NONE ถูกล็อก — กรุณาเลือกโหมด AUTO หรือ MANUAL ก่อน' : 'โหมด AUTO ถูกล็อก — กดได้เฉพาะปุ่มรีเซท');
      return;
    }
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      showToast('warning', 'ระบบอยู่ในสถานะ Timeout (ล็อกอยู่) — สามารถกดได้เฉพาะปุ่ม "รีเซท" เท่านั้น');
      return;
    }
    state.acPower = val;
    if (isUserAction) {
      state.userModifiedPower = true;
    }
    if (DOM.powerBtnOn && DOM.powerBtnOff) {
      DOM.powerBtnOn.classList.toggle('mqtt-power-btn--active', val === 1);
      DOM.powerBtnOff.classList.toggle('mqtt-power-btn--active', val === 0);
    }
  }

  function updateMqttTempDisplay() {
    if (DOM.mqttTempDisplay) {
      const temp = state.targetTemp || 25;
      DOM.mqttTempDisplay.textContent = `${temp}°C`;
    }
  }

  // ============================================================
  //  CONTROLS (SAVE / START / STOP / RESET)
  // ============================================================

  function saveSchedule() {
    if (state.scheduleMode === 'none') {
      showToast('warning', 'โหมด NONE ถูกล็อก — กรุณาเลือกโหมด AUTO หรือ MANUAL ก่อน');
      return;
    }
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      showToast('error', 'ระบบล็อกอยู่ (ไฟแดง) กรุณากดปุ่ม "รีเซท" ก่อน');
      return;
    }

    const isAutoMode = (state.scheduleMode === 'auto');
    const todayIso = getTodayIso();

    let onDateVal, offDateVal, onTimeVal, offTimeVal;

    if (isAutoMode) {
      // Auto mode: fixed 08:00-17:00 using today's date
      onDateVal = todayIso;
      offDateVal = todayIso;
      onTimeVal = '08:00';
      offTimeVal = '17:00';
    } else {
      // Manual mode: use user input
      onDateVal = DOM.onDate?.value || todayIso;
      offDateVal = DOM.offDate?.value || onDateVal;
      onTimeVal = DOM.onTime?.value;
      offTimeVal = DOM.offTime?.value;
    }

    const targetTemp = getValidTargetTemp();

    if (!onTimeVal || !offTimeVal) {
      showToast('error', 'กรุณากำหนดเวลาเปิดและเวลาปิดเครื่องปรับอากาศ');
      return;
    }

    const onIso = parseThaiDateToIso(onDateVal);
    const offIso = parseThaiDateToIso(offDateVal);
    if (offIso < onIso) {
      showToast('error', 'วันที่ปิดเครื่องปรับอากาศต้องไม่เกิดขึ้นก่อนวันที่เปิดเครื่องปรับอากาศ');
      return;
    }

    const modeLabel = isAutoMode ? '[AUTO]' : '[MANUAL]';
    const now = new Date();
    const { start, stop } = getScheduleRange(onDateVal, onTimeVal, offDateVal, offTimeVal);

    if (!start || !stop) {
      showToast('error', 'รูปแบบเวลาไม่ถูกต้อง กรุณากำหนดเวลาใหม่');
      return;
    }

    if (!isAutoMode) {
      // 1. ตรวจสอบห้ามตั้งเวลาเปิดย้อนหลังเกิน 2 นาที (อนุญาตให้เลือกนาทีปัจจุบันได้)
      if (start.getTime() < now.getTime() - 120000) {
        showToast('error', `ห้ามตั้งเวลาเปิดเครื่องปรับอากาศย้อนหลัง (${onTimeVal} ผ่านมาแล้ว) กรุณากำหนดเวลาในอนาคต`);
        state.schedule.enabled = false;
        return;
      }

      // 2. ตรวจสอบห้ามตั้งเวลาปิดย้อนหลัง (ปิดย้อนหลังไม่ได้)
      if (stop.getTime() <= now.getTime()) {
        showToast('error', `ห้ามตั้งเวลาปิดเครื่องปรับอากาศย้อนหลัง (${offTimeVal} ผ่านมาแล้ว) กรุณากำหนดเวลาในอนาคต`);
        state.schedule.enabled = false;
        return;
      }

      // 3. เวลาปิดต้องมากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป (>= 60,000 ms)
      const diffMs = stop.getTime() - start.getTime();
      if (diffMs < 60 * 1000) {
        showToast('error', 'ไม่สามารถบันทึกได้! เวลาปิดเครื่องปรับอากาศต้องตั้งให้มากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป');
        state.schedule.enabled = false;
        return;
      }
    }

    // ผ่านการตรวจสอบเรียบร้อยแล้ว -> เปิดการใช้งานตั้งเวลาและบันทึกค่า
    state.schedule.onDate = onDateVal;
    state.schedule.onTime = onTimeVal;
    state.schedule.offDate = offDateVal;
    state.schedule.offTime = offTimeVal;
    state.schedule.enabled = true;
    saveSettings();

    // ในโหมด MANUAL: ส่งค่า Modbus (D500-D504 + M100=ON + M42=ON) เมื่อกดปุ่มบันทึกค่า
    if (!isAutoMode) {
      sendMqttPayload(0, targetTemp, state.acMode, state.acFan, 0, 0, 0, 0, 0, 0, 1, true, 1);
    }

    // บันทึกค่าสำเร็จ -> ไปสถานะ READY (ไฟเขียวกระพริบ) เสมอ
    state.acOn = false;
    updateSystemState('ready');
    const isFutureDate = (onIso > todayIso);
    const displayWait = isFutureDate ? `${formatDisplayDate(onDateVal)} ${onTimeVal}` : onTimeVal;

    broadcastUiSync('save_schedule');
    addLog('success', `${modeLabel} ตั้งเวลาล่วงหน้าสำเร็จ: ${formatDisplayDate(onDateVal)} ${onTimeVal} - ${formatDisplayDate(offDateVal)} ${offTimeVal} (${targetTemp}°C) (ไฟเขียวกระพริบ รอถึงเวลาเริ่ม)`);
    showToast('success', `${modeLabel} ตั้งเวลาล่วงหน้าสำเร็จ — อุณหภูมิ ${targetTemp}°C (รอถึงเวลา ${displayWait})`);
  }

  function startAC() {
    if (state.scheduleMode === 'none') {
      showToast('warning', 'โหมด NONE ถูกล็อก — กรุณาเลือกโหมด AUTO หรือ MANUAL ก่อน');
      return;
    }
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      showToast('error', 'ไม่สามารถเริ่มทำงานได้! ระบบล็อกอยู่ ต้องกดปุ่ม "รีเซท" ก่อนเท่านั้น');
      return;
    }

    const isAutoMode = (state.scheduleMode === 'auto');

    if (!isAutoMode && !isScheduleSet()) {
      showToast('error', 'กรุณากดปุ่ม "บันทึกค่า" เพื่อตั้งเวลาก่อนกดเริ่มทำงาน');
      return;
    }
    const todayIso = getTodayIso();

    let onDateVal, offDateVal, onTimeVal, offTimeVal;

    if (isAutoMode) {
      onDateVal = todayIso;
      offDateVal = todayIso;
      onTimeVal = '08:00';
      offTimeVal = '17:00';
    } else {
      onDateVal = state.schedule.onDate || DOM.onDate?.value || todayIso;
      offDateVal = state.schedule.offDate || DOM.offDate?.value || onDateVal;
      onTimeVal = state.schedule.onTime || DOM.onTime?.value;
      offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    }

    const targetTemp = getValidTargetTemp();

    const finalOnDate = onDateVal || todayIso;
    const finalOffDate = offDateVal || finalOnDate;

    const onIso = parseThaiDateToIso(finalOnDate);
    const offIso = parseThaiDateToIso(finalOffDate);
    if (offIso < onIso) {
      showToast('error', 'วันที่ปิดเครื่องปรับอากาศต้องไม่เกิดขึ้นก่อนวันที่เปิดเครื่องปรับอากาศ');
      return;
    }

    const modeLabel = isAutoMode ? '[AUTO]' : '[MANUAL]';
    const now = new Date();
    const { start, stop } = getScheduleRange(finalOnDate, onTimeVal, finalOffDate, offTimeVal);

    if (!start || !stop) {
      showToast('error', 'รูปแบบเวลาไม่ถูกต้อง กรุณากำหนดเวลาใหม่');
      return;
    }

    if (!isAutoMode) {
      // 1. ตรวจสอบห้ามตั้งเวลาปิดย้อนหลัง (เวลาปิดต้องอยู่ในอนาคต)
      if (stop.getTime() <= now.getTime()) {
        showToast('error', `เวลาปิดเครื่องปรับอากาศ (${offTimeVal}) ผ่านมาแล้ว กรุณากำหนดเวลาปิดในอนาคต`);
        state.schedule.enabled = false;
        return;
      }

      // 2. เวลาปิดต้องมากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป (>= 60,000 ms)
      const diffMs = stop.getTime() - start.getTime();
      if (diffMs < 60 * 1000) {
        showToast('error', 'ไม่สามารถเริ่มทำงานได้! เวลาปิดเครื่องปรับอากาศต้องตั้งให้มากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป');
        state.schedule.enabled = false;
        return;
      }
    }

    // ผ่านการตรวจสอบเรียบร้อยแล้ว -> เปิดการใช้งานตั้งเวลาและบันทึกค่า
    state.schedule.onDate = finalOnDate;
    state.schedule.onTime = onTimeVal;
    state.schedule.offDate = finalOffDate;
    state.schedule.offTime = offTimeVal;
    state.schedule.enabled = true;

    const isFutureDate = (onIso > todayIso);
    const displayWait = isFutureDate ? `${formatDisplayDate(finalOnDate)} ${onTimeVal}` : onTimeVal;

    // กดเริ่มทำงานสำเร็จ -> ส่งคำสั่งเปิดเครื่องปรับอากาศ M5=ON และ Modbus D10-D14
    if (now >= start) {
      state.acOn = true;
      sendMqttPayload(1, targetTemp, state.acMode, state.acFan, 0, 0, 0, 0, 1); // start_btn = 1 (Triggers M5 ON & IR 10x)
      startIrTransmissionLock(5500);
      updateSystemState('running');
      broadcastUiSync('start_ac');
      addLog('success', `${modeLabel} กดเริ่มทำงาน — อยู่ในช่วงเวลา (${onTimeVal}-${offTimeVal}) สั่งเปิดเครื่องปรับอากาศสำเร็จ (กำลังยิง IR 10 รอบ...)`);
      showToast('success', `${modeLabel} เริ่มทำงานแล้ว — สั่งเปิดเครื่องปรับอากาศ (${onTimeVal}-${offTimeVal})`);
    } else {
      state.acOn = false;
      updateSystemState('ready');
      broadcastUiSync('start_ac');
      addLog('info', `${modeLabel} กดเริ่มทำงานแล้ว — ยังไม่ถึงเวลา (${displayWait}) ตั้งค่า ${targetTemp}°C (ไฟเขียวกระพริบรอเริ่มทำงานตามเวลา)`);
      showToast('info', `${modeLabel} กดเริ่มทำงานแล้ว — ตั้งอุณหภูมิ ${targetTemp}°C (รอถึงเวลา ${displayWait})`);
    }
  }

  function stopAC() {
    if (state.scheduleMode === 'none') return;
    if (state.irTransmitting) {
      showToast('warning', '⏳ กำลังยิงสัญญาณ IR (10 รอบ)... กรุณารอให้สัญญาณยิงครบ 10 รอบก่อน');
      return;
    }
    state.acOn = false;
    sendMqttPayload(0, getValidTargetTemp(), state.acMode, state.acFan, 0, 0, 0, 1); // stop_btn = 1 (Triggers M6 ON & IR 10x OFF)
    startIrTransmissionLock(5500);
    updateSystemState('stopped');
    broadcastUiSync('stop_ac');
    addLog('warning', 'กดหยุดการทำงาน — สั่งปิดเครื่องปรับอากาศและยิงสัญญาณ IR (10 ครั้ง)');
    showToast('warning', 'หยุดทำงานแล้ว — ยิง IR ปิดเครื่องปรับอากาศ (10 ครั้ง)');
  }

  function resetSystem() {
    state.acOn = false;
    state.schedule.enabled = false;
    state.schedule.onTime = '';
    state.schedule.offTime = '';
    state.schedule.onDate = '';
    state.schedule.offDate = '';

    if (DOM.onTime) DOM.onTime.value = '';
    if (DOM.offTime) DOM.offTime.value = '';
    if (DOM.onDate) DOM.onDate.value = '';
    if (DOM.offDate) DOM.offDate.value = '';

    // เมื่อกดรีเซท ให้สลับเด้งกลับสู่โหมด NONE MODE ทันที (ทั้งจาก AUTO และ MANUAL)
    state.scheduleMode = 'none';

    // ปลดล็อคสถานะระบบและ IR timer
    if (state.irTimer) {
      clearTimeout(state.irTimer);
      state.irTimer = null;
    }
    state.irTransmitting = false;
    state.systemState = 'idle';

    // 1. ส่งคำสั่ง reset=1 (ขอบขาขึ้น / Rising Edge) ไปยัง ESP32/PLC
    sendMqttPayload(0, getValidTargetTemp(), state.acMode, state.acFan, 0, 1);
    saveSettings();

    // 2. ขอบขาลง (Falling Edge): คืนค่า reset=0 อัตโนมัติใน 300ms เพื่อให้เป็นพัลส์ขอบขาขึ้นจังหวะเดียว (One-Shot)
    setTimeout(() => {
      if (state.mqttClient && state.mqttClient.connected) {
        sendMqttPayload(0, getValidTargetTemp(), state.acMode, state.acFan, 0, 0);
      }
    }, 300);

    // สลับหน้าจอและการควบคุมเข้าสู่ NONE MODE ทันที
    applyScheduleMode('none');

    broadcastUiSync('reset_system');
    addLog('info', 'รีเซทระบบเรียบร้อย (พัลส์ M7 ขอบขาขึ้น) — เด้งกลับสู่โหมด NONE (เลือกระบบ AUTO/MANUAL เพื่อเริ่ม)');
    showToast('success', 'รีเซทระบบเรียบร้อย — เด้งกลับสู่โหมด NONE');
  }

  // ============================================================
  //  DEMO MODE
  // ============================================================

  function toggleDemo() {
    if (state.demoMode) {
      stopDemo();
    } else {
      startDemo();
    }
  }

  function startDemo() {
    if (state.mqttClient) {
      disconnectMqttBroker();
    }

    state.demoMode = true;
    state.acOn = false;
    state.schedule.enabled = false;
    state.schedule.onTime = '';
    state.schedule.offTime = '';
    state.schedule.onDate = '';
    state.schedule.offDate = '';

    if (DOM.onTime) DOM.onTime.value = '';
    if (DOM.offTime) DOM.offTime.value = '';
    if (DOM.onDate) DOM.onDate.value = '';
    if (DOM.offDate) DOM.offDate.value = '';

    updateSystemState('idle');
    updateConnectionUI('demo');

    if (DOM.demoBtn) DOM.demoBtn.classList.add('btn--active');
    if (DOM.connectBtn) DOM.connectBtn.disabled = true;

    addLog('warning', 'Demo Mode เปิดใช้งาน — ข้อมูลจำลอง');
    showToast('warning', 'Demo Mode เปิดใช้งาน');

    demoUpdate();
    state.demoTimer = setInterval(demoUpdate, CONFIG.demoUpdateInterval);
  }

  function stopDemo() {
    state.demoMode = false;
    clearInterval(state.demoTimer);
    state.demoTimer = null;

    if (DOM.demoBtn) DOM.demoBtn.classList.remove('btn--active');
    if (DOM.connectBtn) DOM.connectBtn.disabled = false;

    updateConnectionUI('disconnected');
    addLog('info', 'Demo Mode ปิดใช้งาน');
    showToast('info', 'Demo Mode ปิดแล้ว');
  }

  function demoUpdate() {
    checkScheduleState(new Date());

    const base1 = 24 + Math.sin(Date.now() / 5000) * 2;
    const base2 = 32 + Math.cos(Date.now() / 4000) * 3;
    const base3 = 19 + Math.sin(Date.now() / 6000) * 1.5;

    const data = {
      power: state.acOn ? 1 : 0,
      temperature: state.targetTemp,
      mode: state.acMode,
      fan: state.acFan,
      esp32_online: true,
      plc_online: false,
      modbus_online: false,
      simulation: true,
      machine_state: state.acOn ? 'running' : 'stopped',
    };

    updateSensor(1, parseFloat((base1 + (Math.random() - 0.5) * 0.4).toFixed(1)));
    updateSensor(2, parseFloat((base2 + (Math.random() - 0.5) * 0.6).toFixed(1)));
    updateSensor(3, parseFloat((base3 + (Math.random() - 0.5) * 0.3).toFixed(1)));
    updateTempBadge();

    handleMqttStatus(data);
  }

  // ============================================================
  //  ACTIVITY LOG
  // ============================================================

  function addLog(level, message) {
    const container = DOM.logContainer;
    if (!container) return;

    const empty = container.querySelector('.log-empty');
    if (empty) empty.remove();

    const now = new Date();
    const time = now.toLocaleTimeString('th-TH', { hour12: false });

    const entry = document.createElement('div');
    entry.className = `log-entry log-entry--${level}`;
    entry.innerHTML = `
      <span class="log-entry__time">${time}</span>
      <span class="log-entry__msg">${escapeHtml(message)}</span>
    `;

    container.prepend(entry);

    const entries = container.querySelectorAll('.log-entry');
    if (entries.length > 50) {
      entries[entries.length - 1].remove();
    }
  }

  function clearLog() {
    if (DOM.logContainer) {
      DOM.logContainer.innerHTML = '<div class="log-empty">ยังไม่มีกิจกรรม...</div>';
    }
  }

  // ============================================================
  //  TOAST NOTIFICATIONS (SINGLE-TOAST ONLY ON USER CLICK)
  // ============================================================

  let currentToastTimeout = null;
  let lastToastMsg = '';
  let lastToastTime = 0;

  function showToast(type, message) {
    if (!DOM.toastContainer) return;
    const now = Date.now();

    // Prevent duplicate toast spamming within 1.5 seconds
    if (message === lastToastMsg && now - lastToastTime < 1500) {
      return;
    }
    lastToastMsg = message;
    lastToastTime = now;

    // Clear any previous toast immediately to show only 1 toast at a time
    if (currentToastTimeout) {
      clearTimeout(currentToastTimeout);
      currentToastTimeout = null;
    }
    DOM.toastContainer.innerHTML = '';

    const icons = {
      info: '💡',
      success: '✅',
      warning: '⚠️',
      error: '❌',
    };

    const toast = document.createElement('div');
    toast.className = `toast toast--${type}`;
    toast.innerHTML = `
      <span class="toast__icon">${icons[type] || '💡'}</span>
      <span class="toast__msg">${escapeHtml(message)}</span>
    `;

    DOM.toastContainer.appendChild(toast);

    currentToastTimeout = setTimeout(() => {
      toast.classList.add('toast--removing');
      setTimeout(() => {
        if (toast.parentElement) toast.remove();
      }, 200);
    }, 2000);
  }

  // ── Utilities ──
  function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  // ── Start ──
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
