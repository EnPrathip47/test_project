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
    topicHistorySync: "aircon/history/sync", // หัวข้อ MQTT ซิงค์ประวัติเซนเซอร์ข้ามเครื่อง (Retained Message)
    topicSettingsSync: "aircon/settings/sync", // หัวข้อ MQTT ซิงค์การตั้งค่าระบบข้ามเครื่อง (Retained Message)
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
    sensors: { temp1: null, temp2: null, temp3: null, lux: null, d10: null },
    sensorsUpdatedAt: null,
    indicators: { power: false, running: false, fault: false },
    // MQTT Remote Control (Desired & Actual State)
    acPower: 0,     // 0=OFF, 1=ON
    acMode: 0,      // 0=AUTO (Fixed as AUTO)
    acFan: 0,       // 0=AUTO, 1=LOW, 2=MED, 3=HIGH
    esp32Online: false,
    plcOnline: false,
    mqttOnline: false,
    lastCommand: '',
    irTransmitting: false,
    irTimer: null,
    lastEsp32Heartbeat: 0,
    preStopWarned: false,

    // User Pending Modifications (Prevent 5s periodic background status overwrite)
    userModifiedPower: false,
    userModifiedMode: false,
    userModifiedFan: false,
    userModifiedTemp: false,
    userModifiedModeUntil: 0,
    userModifiedTempUntil: 0,
    userModifiedPowerUntil: 0,
    userModifiedFanUntil: 0,
    userActionUntil: 0,

    // PLC RTC Time Sync (TRD D400-D406)
    plcRtc: {
      year: 0,
      month: 0,
      day: 0,
      hour: 0,
      minute: 0,
      second: 0,
      dayOfWeek: 0,
      timeStr: '',
      valid: false,
      lastSync: 0,
    },
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
    sensorCardLux: document.getElementById('sensorCardLux'),
    sensorLuxVal: document.getElementById('sensorLuxVal'),
    sensorProgressLux: document.getElementById('sensorProgressLux'),
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
    summaryModeText: document.getElementById('summaryModeText'),
    summaryTimeText: document.getElementById('summaryTimeText'),
    summaryDurationText: document.getElementById('summaryDurationText'),
    summaryProgressText: document.getElementById('summaryProgressText'),

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
    btnResetHint: document.getElementById('btnResetHint'),

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
    historyCloudSyncBadge: document.getElementById('historyCloudSyncBadge'),
    historyCloudSyncText: document.getElementById('historyCloudSyncText'),

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

    // User Manual Modal
    openManualBtn: document.getElementById('openManualBtn'),
    manualModal: document.getElementById('manualModal'),
    manualModalBackdrop: document.getElementById('manualModalBackdrop'),
    closeManualBtn: document.getElementById('closeManualBtn'),
    closeManualFooterBtn: document.getElementById('closeManualFooterBtn'),
    printManualBtn: document.getElementById('printManualBtn'),
    manualModalNav: document.getElementById('manualModalNav'),
  };

  // ── Initialize ──
  function init() {
    setupClock();
    setupParticles();
    initSensors();
    bindEvents();
    loadSettings();
    SensorHistoryManager.init();

    // Set min date for date pickers to today
    const todayIso = getTodayIso();
    if (DOM.onDate) DOM.onDate.min = todayIso;
    if (DOM.offDate) DOM.offDate.min = todayIso;

    applyScheduleMode(state.scheduleMode);

    // Evaluate Real-time State on startup / refresh:
    if (state.scheduleMode === 'auto') {
      const now = new Date();
      const curMins = now.getHours() * 60 + now.getMinutes();
      if (curMins >= 8 * 60 && curMins < 17 * 60) {
        updateSystemState('running');
      } else {
        updateSystemState('ready');
      }
    } else if (state.scheduleMode === 'manual' && state.schedule.enabled) {
      const now = new Date();
      const offD = state.schedule.offDate || todayIso;
      const stopDt = parseScheduleDateTime(offD, state.schedule.offTime);
      if (stopDt) {
        if (state.acOn && now < stopDt) {
          updateSystemState('running');
        } else if (now >= stopDt) {
          updateSystemState('timeout');
        } else {
          updateSystemState('ready');
        }
      } else {
        updateSystemState('ready');
      }
    } else {
      updateSystemState('idle');
    }

    updateMqttStatusUI();
    updateMqttTempDisplay();
    addLog('info', `ระบบพร้อมใช้งาน — ใช้งาน HiveMQ Cloud MQTT over WSS (Port ${CONFIG.mqttWebSocketPort})`);

    // Connect to HiveMQ MQTT Broker
    connectMqttBroker();
  }

  function initSensors() {
    // กำหนดค่าเริ่มต้นสำหรับ UI วงแหวน (SVG Progress Ring)
    // ไม่ฮาร์ดโค้ดค่าเซนเซอร์จำลอง — รอรับค่าจริงจาก ESP32 / PLC ผ่าน MQTT aircon/status เท่านั้น
    for (let i = 1; i <= SENSOR_COUNT; i++) {
      const progressEl = DOM[`sensorProgress${i}`];
      if (progressEl) {
        progressEl.style.strokeDasharray = String(SENSOR_RING_CIRCUMFERENCE);
        progressEl.style.strokeDashoffset = String(SENSOR_RING_CIRCUMFERENCE);
      }
    }
    if (DOM.sensorProgressLux) {
      DOM.sensorProgressLux.style.strokeDasharray = String(SENSOR_RING_CIRCUMFERENCE);
      DOM.sensorProgressLux.style.strokeDashoffset = String(SENSOR_RING_CIRCUMFERENCE);
    }
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
      let now = new Date();

      // โหมด AUTO: ซิงค์เวลาจาก PLC TRD D400-D406 เพื่อความแม่นยำตรงกับ Ladder PLC
      if (state.scheduleMode === 'auto' && state.plcRtc && state.plcRtc.valid && state.plcOnline) {
        const elapsedSec = Math.floor((Date.now() - state.plcRtc.lastSync) / 1000);
        const plcDate = new Date(
          state.plcRtc.year,
          state.plcRtc.month - 1,
          state.plcRtc.day,
          state.plcRtc.hour,
          state.plcRtc.minute,
          state.plcRtc.second + elapsedSec
        );
        if (!isNaN(plcDate.getTime())) {
          now = plcDate;
        }
      }

      if (DOM.headerClock) {
        DOM.headerClock.textContent = now.toLocaleTimeString('th-TH', { hour12: false });
      }
      checkScheduleState(now);
      updateScheduleSummary();
      updateControlButtons();
      checkEsp32Watchdog();
    }
    updateClock();
    setInterval(updateClock, 1000);
  }

  // Web-side ESP32 Watchdog: หากไม่ได้รับสัญญาณจาก ESP32 เกิน 15 วินาที ให้ปรับเป็น OFFLINE
  function checkEsp32Watchdog() {
    if (state.connected && state.esp32Online && state.lastEsp32Heartbeat > 0) {
      const diffMs = Date.now() - state.lastEsp32Heartbeat;
      if (diffMs > 15000) { // เกิน 15 วินาทีไม่มีข้อมูลจาก ESP32
        state.esp32Online = false;
        state.plcOnline = false;
        updateMqttStatusUI();
        addLog('warning', 'ESP32 ขาดการติดต่อ (Watchdog Timeout > 15s) -> ปรับเป็น OFFLINE');
      }
    }
  }

  function checkScheduleState(now) {
    if (state.scheduleMode === 'none') return;
    if (!state.schedule.enabled) return;

    const onTimeVal = state.schedule.onTime || DOM.onTime?.value;
    const onDateVal = state.schedule.onDate || DOM.onDate?.value || getTodayIso();
    const offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    const offDateVal = state.schedule.offDate || DOM.offDate?.value || getTodayIso();

    if (!onTimeVal || !offTimeVal) return;

    // โหมด AUTO: วนลูปอัตโนมัติทุกวัน 08:00 - 17:00 (เมื่อถึงเวลา = เขียวค้าง, นอกเวลา = เขียวกระพริบ)
    if (state.scheduleMode === 'auto') {
      const currentHour = now.getHours();
      const currentMin = now.getMinutes();
      const currentMinutes = currentHour * 60 + currentMin;
      const autoStartMinutes = 8 * 60;   // 08:00
      const autoStopMinutes = 17 * 60;   // 17:00

      // หากผู้ใช้กดปุ่มหยุด (STOPPED) -> ล็อกระบบไว้ ต้องกดปุ่ม "รีเซท" เท่านั้น
      if (state.systemState === 'stopped') {
        return;
      }

      const isInAutoTime = (currentMinutes >= autoStartMinutes && currentMinutes < autoStopMinutes);

      if (isInAutoTime) {
        // เมื่อถึงเวลา (08:00 - 17:00) -> เขียวค้าง (RUNNING)
        if (state.systemState !== 'running') {
          state.acOn = true;
          updateSystemState('running');
          sendMqttPayload(1, getValidTargetTemp(), 0, state.acFan, 0, 0, 0, 0, 1); // start_btn = 1
          startIrTransmissionLock(5500);
          addLog('success', `[AUTO] ถึงเวลาเริ่มทำงาน (08:00) -> สั่งเปิดเครื่องปรับอากาศและยิงสัญญาณ IR อัตโนมัติ`);
          showToast('success', `⏰ ถึงเวลาเปิดเครื่องปรับอากาศแล้ว (08:00) — เริ่มทำงานอัตโนมัติ`);
          broadcastUiSync('start_ac');
        } else {
          // แจ้งเตือนล่วงหน้า 5 นาที (16:55)
          const autoRemainMin = autoStopMinutes - currentMinutes;
          if (autoRemainMin <= 5 && autoRemainMin > 0 && !state.preStopWarned) {
            state.preStopWarned = true;
            showToast('warning', '⏳ แจ้งเตือน: เหลือเวลาทำงานอีก 5 นาที เครื่องปรับอากาศจะหยุดทำงานอัตโนมัติ (17:00)');
            addLog('warning', '[แจ้งเตือน] เหลือเวลาทำงานอีก 5 นาที — จะหยุดทำงานเวลา 17:00');
          }
        }
      } else {
        // นอกเวลา (ก่อน 08:00 หรือ หลัง 17:00) -> นอกเวลาเขียวจะกระพริบ (READY)
        state.preStopWarned = false;
        if (state.systemState === 'running') {
          state.acOn = false;
          sendMqttPayload(0, getValidTargetTemp(), 0, state.acFan, 1);
          startIrTransmissionLock(5500);
          addLog('info', `[AUTO] ครบเวลาทำงาน (17:00) -> ปิดเครื่องปรับอากาศ และเข้าสู่สถานะรอนอกเวลา (เขียวกระพริบ)`);
          showToast('info', `⏰ ครบเวลาทำงานแล้ว (17:00) — ปิดเครื่องปรับอากาศ และเข้าสู่สถานะรอนอกเวลา (เขียวกระพริบ)`);
        }
        if (state.systemState !== 'ready') {
          state.acOn = false;
          updateSystemState('ready');
        }
      }
      return;
    }

    // โหมด MANUAL: ตรวจสอบเวลาเริ่ม (Start) และครบเวลาทำงาน (Stop / TIMEOUT)
    const start = parseScheduleDateTime(onDateVal, onTimeVal);
    const stop = parseScheduleDateTime(offDateVal, offTimeVal);
    if (!start || !stop) return;

    // หากอยู่ในสถานะ STOPPED (กดหยุด) หรือ TIMEOUT (ครบเวลา) -> ล็อกระบบไว้ ต้องกดปุ่ม "รีเซท" เท่านั้น!
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      return;
    }

    if (state.systemState === 'ready' || state.systemState === 'idle') {
      if (now >= start && now < stop) {
        state.acOn = true;
        updateSystemState('running');
        sendMqttPayload(1, getValidTargetTemp(), 0, state.acFan, 0, 0, 0, 0, 1); // start_btn = 1
        startIrTransmissionLock(5500);
        addLog('success', `[Auto-Start] ถึงเวลาเริ่มทำงาน (${onTimeVal}) -> สั่งเปิดเครื่องปรับอากาศและยิงสัญญาณ IR อัตโนมัติ`);
        showToast('success', `⏰ ถึงเวลาเปิดเครื่องปรับอากาศแล้ว (${onTimeVal}) — เริ่มทำงานอัตโนมัติ`);
        broadcastUiSync('start_ac');
      }
    } else if (state.systemState === 'running') {
      const remainMs = stop.getTime() - now.getTime();
      if (remainMs > 0 && remainMs <= 5 * 60 * 1000 && !state.preStopWarned) {
        state.preStopWarned = true;
        showToast('warning', `⏳ แจ้งเตือน: เหลือเวลาทำงานอีก 5 นาที เครื่องปรับอากาศจะหยุดทำงานอัตโนมัติ (${offTimeVal})`);
        addLog('warning', `[แจ้งเตือน] เหลือเวลาทำงานอีก 5 นาที — จะหยุดทำงานเวลา ${offTimeVal}`);
      }
      if (now >= stop) {
        state.acOn = false;
        updateSystemState('timeout');
        sendMqttPayload(0, getValidTargetTemp(), 0, state.acFan, 1); // [ Complete Flag set M500 ]
        startIrTransmissionLock(5500);
        addLog('warning', `[Schedule] ครบเวลาทำงาน (${offTimeVal}) -> ส่งคำสั่งปิดเครื่องปรับอากาศและยิงสัญญาณ IR`);
        showToast('warning', `🛑 ทำงานครบเวลาแล้ว (${offTimeVal}) — ปิดเครื่องปรับอากาศเรียบร้อย (กรุณากดรีเซทเพื่อเริ่มรอบใหม่)`);
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

  // Click Throttling Helper (Prevents rapid double-clicking / jitter)
  function throttleClick(fn, delay = 350) {
    let lastCall = 0;
    return function (...args) {
      const now = Date.now();
      if (now - lastCall < delay) return;
      lastCall = now;
      return fn.apply(this, args);
    };
  }

  // ── Event Binding ──
  function bindEvents() {
    DOM.btnSave?.addEventListener('click', throttleClick(saveSchedule));
    DOM.btnStart?.addEventListener('click', throttleClick(startAC));
    DOM.btnStop?.addEventListener('click', throttleClick(stopAC));
    DOM.btnReset?.addEventListener('click', throttleClick(resetSystem));
    DOM.connectBtn?.addEventListener('click', throttleClick(connectMqttBroker));
    DOM.disconnectBtn?.addEventListener('click', throttleClick(disconnectMqttBroker));
    DOM.demoBtn?.addEventListener('click', throttleClick(toggleDemo));
    DOM.clearLogBtn?.addEventListener('click', throttleClick(clearLog));

    DOM.onDate?.addEventListener('change', () => {
      validateDateNotPast(DOM.onDate);
      state.schedule.onDate = DOM.onDate.value;
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
      validateTimeInterval();
      state.schedule.onTime = DOM.onTime.value;
      saveSettings();
      broadcastUiSync('change_input');
    });

    DOM.offDate?.addEventListener('change', () => {
      validateDateNotPast(DOM.offDate);
      state.schedule.offDate = DOM.offDate.value;
      saveSettings();
      updateScheduleInputsState();
      broadcastUiSync('change_input');
    });

    DOM.offTime?.addEventListener('input', () => {
      state.schedule.offTime = DOM.offTime.value;
      saveSettings();
      updateScheduleInputsState();
      broadcastUiSync('change_input');
    });
    DOM.offTime?.addEventListener('change', () => {
      validateTimeInterval();
      state.schedule.offTime = DOM.offTime.value;
      saveSettings();
      broadcastUiSync('change_input');
    });

    DOM.targetTemp?.addEventListener('change', () => {
      state.userModifiedTemp = true;
      const validVal = getValidTargetTemp();
      updateMqttTempDisplay();
      saveSettings();
      broadcastUiSync('change_control');
      const inWindow = isCurrentlyInWorkingWindow();
      const isRunning = (state.systemState === 'running' || state.acOn) && inWindow;
      if (isRunning) {
        sendMqttPayload(1, validVal, 0, state.acFan, 0, 0, 1, 0);
        startIrTransmissionLock(5500);
      }
    });
    DOM.targetTemp?.addEventListener('input', () => {
      state.userModifiedTemp = true;
      updateMqttTempDisplay();
      broadcastUiSync('change_control');
    });
    DOM.targetTemp?.addEventListener('blur', () => {
      const validVal = getValidTargetTemp();
      updateMqttTempDisplay();
      const inWindow = isCurrentlyInWorkingWindow();
      const isRunning = (state.systemState === 'running' || state.acOn) && inWindow;
      if (isRunning) {
        sendMqttPayload(1, validVal, 0, state.acFan, 0, 0, 1, 0);
        startIrTransmissionLock(5500);
      }
    });

    // Temp +/- Controls
    DOM.tempMinusBtn?.addEventListener('click', throttleClick(() => adjustTempStep(-1), 200));
    DOM.tempPlusBtn?.addEventListener('click', throttleClick(() => adjustTempStep(1), 200));

    // Temp Presets Chips (18°C, 20°C, 22°C, 24°C, 25°C, 27°C)
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

    DOM.fanSelect?.addEventListener('change', () => {
      if (state.scheduleMode === 'none') return;
      state.acFan = parseInt(DOM.fanSelect.value, 10);
      state.userModifiedFan = true;
      state.userModifiedFanUntil = Date.now() + 5000;
      broadcastUiSync('change_control');
      const inWindow = isCurrentlyInWorkingWindow();
      const isRunning = (state.systemState === 'running' || state.acOn) && inWindow;
      if (isRunning) {
        sendMqttPayload(1, getValidTargetTemp(), 0, state.acFan, 0, 0, 1, 0);
        startIrTransmissionLock(5500);
      }
    });

    DOM.btnSendMqtt?.addEventListener('click', throttleClick(sendMqttCommandFromUI));

    // None/Auto/Manual Mode Toggle (Throttled & Optimistic Lock)
    DOM.modeNoneBtn?.addEventListener('click', throttleClick(() => setScheduleMode('none')));
    DOM.modeAutoBtn?.addEventListener('click', throttleClick(() => setScheduleMode('auto')));
    DOM.modeManualBtn?.addEventListener('click', throttleClick(() => setScheduleMode('manual')));

    // User Manual Modal Events
    DOM.openManualBtn?.addEventListener('click', openManualModal);
    DOM.closeManualBtn?.addEventListener('click', closeManualModal);
    DOM.closeManualFooterBtn?.addEventListener('click', closeManualModal);
    DOM.manualModalBackdrop?.addEventListener('click', closeManualModal);
    DOM.printManualBtn?.addEventListener('click', () => {
      const prevOverflow = document.body.style.overflow;
      document.body.style.overflow = 'visible';
      window.print();
      setTimeout(() => {
        if (DOM.manualModal?.classList.contains('manual-modal--open')) {
          document.body.style.overflow = 'hidden';
        }
      }, 500);
    });

    window.addEventListener('beforeprint', () => {
      document.body.style.overflow = 'visible';
    });

    window.addEventListener('afterprint', () => {
      if (DOM.manualModal?.classList.contains('manual-modal--open')) {
        document.body.style.overflow = 'hidden';
      }
    });

    // Navigation Tab Switching inside Manual Modal
    document.querySelectorAll('.manual-nav-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const tabId = btn.getAttribute('data-tab');
        if (tabId) switchManualTab(tabId);
      });
    });

    // Close Modal on Escape key
    window.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && DOM.manualModal?.classList.contains('manual-modal--open')) {
        closeManualModal();
      }
    });
  }

  // ============================================================
  //  USER MANUAL MODAL CONTROLLER
  // ============================================================

  function openManualModal() {
    if (!DOM.manualModal) return;
    DOM.manualModal.classList.add('manual-modal--open');
    DOM.manualModal.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden'; // Prevent background scrolling
  }

  function closeManualModal() {
    if (!DOM.manualModal) return;
    DOM.manualModal.classList.remove('manual-modal--open');
    DOM.manualModal.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = '';
  }

  function switchManualTab(tabId) {
    // Update active state on nav buttons
    document.querySelectorAll('.manual-nav-btn').forEach((btn) => {
      if (btn.getAttribute('data-tab') === tabId) {
        btn.classList.add('manual-nav-btn--active');
      } else {
        btn.classList.remove('manual-nav-btn--active');
      }
    });

    // Update active state on tab panes
    document.querySelectorAll('.manual-tab-pane').forEach((pane) => {
      if (pane.id === tabId) {
        pane.classList.add('manual-tab-pane--active');
      } else {
        pane.classList.remove('manual-tab-pane--active');
      }
    });

    // Scroll modal body to top smoothly on tab switch
    const modalBody = document.getElementById('manualModalBody');
    if (modalBody) modalBody.scrollTo({ top: 0, behavior: 'smooth' });
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
      state.userModifiedTempUntil = Date.now() + 5000;
      broadcastUiSync('change_control');
      const inWindow = isCurrentlyInWorkingWindow();
      const isRunning = (state.systemState === 'running' || state.acOn) && inWindow;

      if (isRunning) {
        sendMqttPayload(1, validVal, 0, state.acFan, 0, 0, 1, 0);
        startIrTransmissionLock(5500);
        showToast('success', `ส่งค่าอุณหภูมิ ${validVal}°C ไปยังแอร์แล้ว (กำลังยิง IR 10 รอบ...)`);
      } else {
        // ยังไม่ถึงเวลาเริ่มทำงาน หรือยังไม่รัน -> บันทึกค่าอุณหภูมิไว้เพื่อรอเวลาเริ่ม โดยไม่ยิงไปแอร์
        showToast('info', `ตั้งค่าอุณหภูมิเป้าหมาย ${validVal}°C สำเร็จ (จะเริ่มทำงานเมื่อถึงเวลาที่กำหนด)`);
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

  function validateTimeInterval(silent = false) {
    if (state.scheduleMode !== 'manual') return true;
    const onTimeVal = DOM.onTime?.value || state.schedule.onTime;
    const offTimeVal = DOM.offTime?.value || state.schedule.offTime;
    const onDateVal = DOM.onDate?.value || state.schedule.onDate || getTodayIso();
    const offDateVal = DOM.offDate?.value || state.schedule.offDate || getTodayIso();
    if (!onTimeVal || !offTimeVal) return true;

    const start = parseScheduleDateTime(onDateVal, onTimeVal);
    const stop = parseScheduleDateTime(offDateVal, offTimeVal);
    if (!start || !stop) return true;

    const now = new Date();
    // กฎที่ 1: เวลาเริ่มต้องมากกว่าเวลาปัจจุบันอย่างน้อย 1 นาที
    const minStart = new Date(now.getTime() + 60 * 1000);
    if (start.getTime() < minStart.getTime()) {
      if (!silent) {
        showToast('warning', `⚠️ เวลาเริ่มเปิดเครื่องต้องมากกว่าเวลาปัจจุบันอย่างน้อย 1 นาที (ปัจจุบัน ${now.toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit', hour12: false })})`);
      }
      return false;
    }

    // กฎที่ 2: เวลาหยุดขั้นต่ำ 5 นาที
    const minStop = new Date(start.getTime() + 5 * 60 * 1000);
    if (stop.getTime() < minStop.getTime()) {
      if (!silent) {
        showToast('warning', '⚠️ เวลาหยุดทำงานขั้นต่ำต้องห่างจากเวลาเริ่มอย่างน้อย 5 นาที');
      }
      return false;
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
        state.acMode = 0; // Fixed as AUTO
        if (settings.acFan != null) state.acFan = Number(settings.acFan);
        if (settings.acPower != null) state.acPower = Number(settings.acPower);
        if (settings.acOn != null) state.acOn = Boolean(settings.acOn);

        if (state.scheduleMode === 'manual') {
          state.schedule.onDate = settings.onDate || '';
          state.schedule.onTime = settings.onTime || '';
          state.schedule.offDate = settings.offDate || '';
          state.schedule.offTime = settings.offTime || '';
          state.schedule.enabled = Boolean(settings.scheduleEnabled);
          if (state.schedule.enabled && settings.systemState) {
            state.systemState = settings.systemState;
          } else {
            state.systemState = 'idle';
          }
        } else if (state.scheduleMode === 'auto') {
          const todayIso = getTodayIso();
          state.schedule.onDate = todayIso;
          state.schedule.onTime = '08:00';
          state.schedule.offDate = todayIso;
          state.schedule.offTime = '17:00';
          state.schedule.enabled = true;
          if (settings.systemState) {
            state.systemState = settings.systemState;
          }
        } else {
          // In NONE mode:
          state.schedule.enabled = false;
          state.schedule.onDate = '';
          state.schedule.onTime = '';
          state.schedule.offDate = '';
          state.schedule.offTime = '';
          state.systemState = 'idle';
        }
      } catch (e) {
        console.warn('Failed to load settings:', e);
      }
    }
  }

  function saveSettings(broadcastCloud = true) {
    try {
      const settings = {
        senderId: state.clientId,
        updatedAt: Date.now(),
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
        acMode: 0, // Fixed as AUTO
        acFan: state.acFan
      };
      localStorage.setItem('airCandySettings', JSON.stringify(settings));

      if (broadcastCloud && state.mqttClient && state.mqttClient.connected) {
        try {
          state.mqttClient.publish(CONFIG.topicSettingsSync, JSON.stringify(settings), { qos: 1, retain: true });
        } catch (e) { }
      }
    } catch (e) {
      console.warn('Failed to save settings:', e);
    }
  }

  function applyRemoteSettings(settings) {
    if (!settings || typeof settings !== 'object') return;
    if (settings.senderId === state.clientId) return;

    if (settings.scheduleMode && ['none', 'auto', 'manual'].includes(settings.scheduleMode)) {
      if (state.scheduleMode !== settings.scheduleMode) {
        state.scheduleMode = settings.scheduleMode;
        applyScheduleMode(settings.scheduleMode);
      }
    }

    if (settings.scheduleEnabled !== undefined) {
      state.schedule.enabled = Boolean(settings.scheduleEnabled);
    }

    if (settings.onDate !== undefined) {
      state.schedule.onDate = settings.onDate;
      if (DOM.onDate && document.activeElement !== DOM.onDate) DOM.onDate.value = settings.onDate;
    }
    if (settings.onTime !== undefined) {
      state.schedule.onTime = settings.onTime;
      if (DOM.onTime && document.activeElement !== DOM.onTime) DOM.onTime.value = settings.onTime;
    }
    if (settings.offDate !== undefined) {
      state.schedule.offDate = settings.offDate;
      if (DOM.offDate && document.activeElement !== DOM.offDate) DOM.offDate.value = settings.offDate;
    }
    if (settings.offTime !== undefined) {
      state.schedule.offTime = settings.offTime;
      if (DOM.offTime && document.activeElement !== DOM.offTime) DOM.offTime.value = settings.offTime;
    }

    if (settings.targetTemp != null) {
      const t = parseFloat(settings.targetTemp);
      if (!isNaN(t) && t >= 18 && t <= 27) {
        state.targetTemp = t;
        if (DOM.targetTemp && document.activeElement !== DOM.targetTemp) DOM.targetTemp.value = t;
        document.querySelectorAll('.temp-chip').forEach((chip) => {
          const chipVal = parseFloat(chip.getAttribute('data-temp'));
          chip.classList.toggle('temp-chip--active', chipVal === t);
        });
        updateMqttTempDisplay();
      }
    }

    if (settings.acFan != null && DOM.fanSelect) {
      state.acFan = Number(settings.acFan);
      DOM.fanSelect.value = state.acFan;
    }

    updateScheduleSummary();
    updateScheduleInputsState();
    updateControlButtons();
    saveSettings(false);
  }

  // ============================================================
  //  AUTO / MANUAL SCHEDULE MODE
  // ============================================================

  function setScheduleMode(mode) {
    // เมื่อกด STOP หรืออยู่ในสถานะ STOPPED / TIMEOUT (ไฟแดงติดค้าง) ห้ามไปโหมดอื่นจนกว่าจะกดรีเซทเท่านั้น
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      showToast('warning', '🔒 ระบบถูกล็อกจากปุ่ม STOP (ไฟแดงติดค้าง) — ต้องกดปุ่ม "รีเซท" ก่อนเท่านั้นถึงจะเปลี่ยนโหมดได้');
      addLog('warning', `[Mode] ไม่สามารถเปลี่ยนโหมดได้ — ระบบอยู่ในสถานะ STOPPED (ต้องกดรีเซทเท่านั้น)`);
      return;
    }

    // เมื่อเลือกโหมด AUTO หรือ MANUAL แล้ว จะไม่สามารถเปลี่ยนโหมดได้จนกว่าจะกดปุ่ม "รีเซท" เพื่อกลับไปโหมด NONE
    if (state.scheduleMode !== 'none' && mode !== state.scheduleMode) {
      showToast('warning', '🔒 ล็อคโหมดการทำงานแล้ว — ต้องกดปุ่ม "รีเซท" เพื่อกลับไปโหมด NONE ก่อนเลือกโหมดใหม่');
      addLog('warning', `[Mode] ไม่สามารถสลับเป็นโหมด ${mode.toUpperCase()} ได้ — ต้องกดปุ่มรีเซทเพื่อกลับไปโหมด NONE ก่อน`);
      return;
    }
    if (state.scheduleMode === mode) {
      return;
    }

    state.userModifiedModeUntil = Date.now() + 5000;
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
        DOM.modeInfoDesc.textContent = 'โหมด MANUAL | ไฟสีเหลืองกระพริบ (ปรับตั้งวันเวลา และกดบันทึกค่า)';
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
      state.schedule.onTime = '';
      state.schedule.offDate = '';
      state.schedule.offTime = '';
      state.acOn = false;
      state.acPower = 0;

      // เคลียร์ค่า input และ display ทั้งหมดให้ว่างเปล่า ไม่มีค่าเดิมค้าง
      if (DOM.onDate) { DOM.onDate.value = ''; DOM.onDate.disabled = true; }
      if (DOM.onTime) { DOM.onTime.value = ''; DOM.onTime.disabled = true; }
      if (DOM.offDate) { DOM.offDate.value = ''; DOM.offDate.disabled = true; }
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
      updateSystemState('idle');
    } else if (isAuto) {
      const todayIso = getTodayIso();
      if (DOM.onTime) { DOM.onTime.value = '08:00'; DOM.onTime.disabled = true; }
      if (DOM.onDate) { DOM.onDate.value = todayIso; DOM.onDate.disabled = true; }
      if (DOM.offTime) { DOM.offTime.value = '17:00'; DOM.offTime.disabled = true; }
      if (DOM.offDate) { DOM.offDate.value = todayIso; DOM.offDate.disabled = true; }
      if (DOM.targetTemp) {
        DOM.targetTemp.disabled = false;
        DOM.targetTemp.value = state.targetTemp ? String(state.targetTemp) : '24';
        DOM.targetTemp.placeholder = '18 - 27';
      }
      updateMqttTempDisplay();

      state.schedule.onDate = todayIso;
      state.schedule.onTime = '08:00';
      state.schedule.offDate = todayIso;
      state.schedule.offTime = '17:00';
      state.schedule.enabled = true;

      const now = new Date();
      const currentHour = now.getHours();
      const currentMin = now.getMinutes();
      const currentMinutes = currentHour * 60 + currentMin;
      const autoStartMinutes = 8 * 60;   // 08:00
      const autoStopMinutes = 17 * 60;   // 17:00

      if (currentMinutes >= autoStartMinutes && currentMinutes < autoStopMinutes) {
        state.acOn = true;
        updateSystemState('running');
      } else {
        state.acOn = false;
        updateSystemState('ready');
      }
    } else {
      // In manual mode:
      if (!state.schedule.enabled) {
        if (DOM.onDate) {
          DOM.onDate.disabled = false;
          DOM.onDate.value = state.schedule.onDate || '';
        }
        if (DOM.onTime) {
          DOM.onTime.disabled = false;
          DOM.onTime.value = state.schedule.onTime || '';
        }
        if (DOM.offDate) {
          DOM.offDate.disabled = false;
          DOM.offDate.value = state.schedule.offDate || '';
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
        updateSystemState('idle');
      } else {
        // If schedule already enabled (setting time แล้ว), restore input values and lock inputs!
        if (DOM.onDate) {
          DOM.onDate.value = state.schedule.onDate || '';
          DOM.onDate.disabled = true;
        }
        if (DOM.onTime) {
          DOM.onTime.value = state.schedule.onTime || '';
          DOM.onTime.disabled = true;
        }
        if (DOM.offDate) {
          DOM.offDate.value = state.schedule.offDate || '';
          DOM.offDate.disabled = true;
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
        const offD = state.schedule.offDate || todayIso;
        const start = parseScheduleDateTime(onD, state.schedule.onTime);
        const stop = parseScheduleDateTime(offD, state.schedule.offTime);
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
      acMode: 0,
      acFan: state.acFan,
      systemState: state.systemState,
      onDate: DOM.onDate?.value || state.schedule.onDate || '',
      onTime: DOM.onTime?.value || state.schedule.onTime || '',
      offDate: DOM.offDate?.value || state.schedule.offDate || '',
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
      if (typeof SensorHistoryManager !== 'undefined' && SensorHistoryManager.records.length > 0) {
        SensorHistoryManager.publishCloudHistory();
      }
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
      state.schedule.onDate = '';
      state.schedule.offTime = '';
      state.schedule.offDate = '';

      if (DOM.onTime) DOM.onTime.value = '';
      if (DOM.onDate) DOM.onDate.value = '';
      if (DOM.offTime) DOM.offTime.value = '';
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
    if (data.onTime !== undefined) {
      state.schedule.onTime = data.onTime;
      if (DOM.onTime && document.activeElement !== DOM.onTime) DOM.onTime.value = data.onTime;
    }
    if (data.offDate !== undefined) {
      state.schedule.offDate = data.offDate;
      if (DOM.offDate && document.activeElement !== DOM.offDate) DOM.offDate.value = data.offDate;
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
    state.acMode = 0; // Fixed as AUTO
    if (data.acFan !== undefined) {
      state.acFan = parseInt(data.acFan, 10);
      if (DOM.fanSelect && document.activeElement !== DOM.fanSelect) DOM.fanSelect.value = state.acFan;
    }

    // 5. Update System State
    if (state.scheduleMode === 'none' || (state.scheduleMode === 'manual' && !state.schedule.enabled)) {
      updateSystemState('idle');
    } else if (data.systemState && data.systemState !== state.systemState) {
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
        keepalive: 30,
        reconnectPeriod: CONFIG.reconnectDelay || 2000,
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

        state.mqttClient.subscribe(CONFIG.topicStatus, { qos: 1 });
        state.mqttClient.subscribe(CONFIG.topicAvailability, { qos: 1 });
        state.mqttClient.subscribe(CONFIG.topicControl, { qos: 1 });
        state.mqttClient.subscribe(CONFIG.topicSync, { qos: 1 }, () => {
          try {
            state.mqttClient.publish(CONFIG.topicSync, JSON.stringify({ type: 'request_sync', senderId: state.clientId }));
          } catch (e) { }
        });
        state.mqttClient.subscribe(CONFIG.topicHistorySync, { qos: 1 });
        state.mqttClient.subscribe(CONFIG.topicSettingsSync, { qos: 1 });

        startPresenceTimer();

        addLog('success', 'เชื่อมต่อ HiveMQ Cloud MQTT Over WSS สำเร็จ!');
        showToast('success', 'เชื่อมต่อ HiveMQ MQTT สำเร็จ (เปิดใช้งานคลาวด์ซิงค์)');
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
          } else if (topic === CONFIG.topicHistorySync) {
            try {
              if (msgStr) {
                const histData = JSON.parse(msgStr);
                if (histData && Array.isArray(histData.records) && typeof SensorHistoryManager !== 'undefined') {
                  SensorHistoryManager.mergeRemoteRecords(histData.records);
                }
              }
            } catch (e) {
              console.warn('[MQTT History Sync] Parse error:', e);
            }
          } else if (topic === CONFIG.topicSettingsSync) {
            try {
              if (msgStr) {
                const settingsData = JSON.parse(msgStr);
                if (settingsData && settingsData.senderId !== state.clientId) {
                  applyRemoteSettings(settingsData);
                }
              }
            } catch (e) {
              console.warn('[MQTT Settings Sync] Parse error:', e);
            }
          } else if (topic === CONFIG.topicSync || topic === CONFIG.topicControl) {
            try {
              const syncData = JSON.parse(msgStr);
              if (syncData && typeof syncData === 'object') {
                if (syncData.type === 'presence') {
                  state.activeUsers[syncData.clientId] = Date.now();
                  updateActiveUsersCount();
                } else if (syncData.type === 'new_sensor_record') {
                  if (syncData.senderId !== state.clientId && syncData.record && typeof SensorHistoryManager !== 'undefined') {
                    SensorHistoryManager.onRemoteRecordReceived(syncData.record);
                  }
                } else if (syncData.type === 'clear_sensor_history') {
                  if (typeof SensorHistoryManager !== 'undefined') {
                    SensorHistoryManager.onRemoteClearReceived();
                  }
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
    const m = Number(mode);
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

    // Calculate Start Date and Stop Date for Manual / Scheduled mode
    let startDt = null;
    let stopDt = null;

    const onTimeVal = state.schedule.onTime || DOM.onTime?.value;
    const onDateVal = state.schedule.onDate || DOM.onDate?.value || getTodayIso();
    const offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    const offDateVal = state.schedule.offDate || DOM.offDate?.value || getTodayIso();

    if (onTimeVal && state.scheduleMode !== 'auto') {
      startDt = parseScheduleDateTime(onDateVal, onTimeVal);
    }
    if (offTimeVal && state.scheduleMode !== 'auto' && includeStopDate) {
      stopDt = parseScheduleDateTime(offDateVal, offTimeVal);
    }

    const payloadObj = {
      power: p,
      temperature: t,
      mode: 0, // Fixed as AUTO (0)
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

      // Start DateTime (ประมวลผลบนเว็บ/ESP32 โดยไม่ส่ง D600-D605 ไป PLC)
      start_year: startDt ? startDt.getFullYear() : 0,
      start_month: startDt ? startDt.getMonth() + 1 : 0,
      start_day: startDt ? startDt.getDate() : 0,
      start_hour: startDt ? startDt.getHours() : 0,
      start_minute: startDt ? startDt.getMinutes() : 0,

      // D500 - D504: Stop DateTime (เวลาปิดเครื่องบน PLC)
      stop_year: stopDt ? stopDt.getFullYear() : 0,
      stop_month: stopDt ? stopDt.getMonth() + 1 : 0,
      stop_day: stopDt ? stopDt.getDate() : 0,
      stop_hour: stopDt ? stopDt.getHours() : 0,
      stop_minute: stopDt ? stopDt.getMinutes() : 0
    };
    const payloadStr = JSON.stringify(payloadObj);

    const fanNames = ['AUTO', 'LOW', 'MEDIUM', 'HIGH'];
    const summary = `Power=${p ? 'ON' : 'OFF'} Temp=${t}°C Mode=AUTO Fan=${fanNames[f]}`;

    // Reset user pending modification flags after sending command
    state.userModifiedPower = false;
    state.userModifiedMode = false;
    state.userModifiedFan = false;
    state.userModifiedTemp = false;

    if (state.mqttClient && state.mqttClient.connected) {
      state.mqttClient.publish(CONFIG.topicControl, payloadStr, { qos: 1 });
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

    const now = new Date();
    const todayIso = getTodayIso();

    if (state.scheduleMode === 'auto') {
      const curMins = now.getHours() * 60 + now.getMinutes();
      return (curMins >= 8 * 60 && curMins < 17 * 60);
    }

    const onTimeVal = state.schedule.onTime || DOM.onTime?.value;
    const onDateVal = state.schedule.onDate || DOM.onDate?.value || todayIso;
    const offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    const offDateVal = state.schedule.offDate || DOM.offDate?.value || todayIso;

    if (!onTimeVal || !offTimeVal) return (state.systemState === 'running' && state.acOn);

    const start = parseScheduleDateTime(onDateVal, onTimeVal);
    const stop = parseScheduleDateTime(offDateVal, offTimeVal);
    if (!start || !stop) return (state.systemState === 'running' && state.acOn);

    return (now >= start && now < stop);
  }

  function sendMqttCommandFromUI() {
    if (state.scheduleMode === 'none') {
      showToast('warning', 'โหมด NONE ถูกล็อก — กรุณาเลือกโหมด AUTO หรือ MANUAL');
      return;
    }
    if (state.irTransmitting) {
      showToast('warning', '⏳ กำลังยิงสัญญาณ IR (10 รอบ)... กรุณารอให้สัญญาณยิงครบ 10 รอบก่อนส่งคำสั่งใหม่');
      return;
    }

    const inWorkingWindow = isCurrentlyInWorkingWindow();
    if (!inWorkingWindow) {
      showToast('warning', '⏳ ยังไม่ถึงเวลาเริ่มทำงาน — ไม่สามารถเปิดแอร์หรือส่งค่าอุณหภูมิไปยังแอร์ได้');
      return;
    }

    const isRunning = (state.systemState === 'running' || state.acOn);
    const temp = getValidTargetTemp();
    const mode = 0; // Fixed as AUTO
    const fan = state.acFan;

    if (isRunning) {
      // อยู่ในช่วงเวลาทำงานและเครื่องปรับอากาศกำลังทำงาน -> ส่ง mqtt_send = 1 เพื่อยิง IR 10 รอบเปลี่ยนอุณหภูมิเครื่องปรับอากาศจริง
      const success = sendMqttPayload(1, temp, mode, fan, 0, 0, 1, 0);
      if (success) {
        startIrTransmissionLock(5500);
        showToast('success', `📡 ส่งค่าอุณหภูมิ ${temp}°C สำเร็จ (กำลังยิง IR 10 รอบ...)`);
        addLog('success', `[MQTT] ส่งค่าอุณหภูมิ ${temp}°C — กำลังยิง IR 10 รอบ`);
      }
    } else {
      // อยู่ในช่วงเวลาทำงานแต่เครื่องยังไม่ได้รัน
      const success = sendMqttPayload(0, temp, mode, fan, 0, 0, 1, 0);
      if (success) {
        showToast('success', `📡 ส่งค่าอุณหภูมิ ${temp}°C สำเร็จ`);
        addLog('success', `[MQTT] ส่งค่าอุณหภูมิ ${temp}°C สำเร็จ`);
      }
    }
  }

  // Handle incoming status payload from ESP32 (Actual State Readback)
  function handleMqttStatus(mqttData) {
    if (!mqttData || typeof mqttData !== 'object') return;
    const now = Date.now();

    // Real-Time Mode Readback from PLC Coils (M9=NONE, M100=MANUAL, M101=AUTO) with 5s Optimistic Lock
    if (now > state.userModifiedModeUntil) {
      let plcMode = null;
      if (mqttData.m101_auto === 1 || mqttData.mode_auto === 1 || (mqttData.schedule_mode && mqttData.schedule_mode.toLowerCase() === 'auto')) {
        plcMode = 'auto';
      } else if (mqttData.m100_manual === 1 || mqttData.mode_manual === 1 || (mqttData.schedule_mode && mqttData.schedule_mode.toLowerCase() === 'manual')) {
        plcMode = 'manual';
      } else if (mqttData.m9_none === 1 || mqttData.mode_none === 1 || (mqttData.schedule_mode && mqttData.schedule_mode.toLowerCase() === 'none')) {
        plcMode = 'none';
      }

      if (plcMode && state.scheduleMode !== plcMode) {
        state.scheduleMode = plcMode;
        applyScheduleMode(plcMode);
        updateScheduleInputsState();
      }
    }

    // HMI Manual Mode M5 Start Trigger Synchronization (อิงเวลาเริ่มตามเวลากด M5 บน HMI)
    if (state.scheduleMode === 'manual' && (mqttData.m5_start === 1 || (mqttData.start_hour !== undefined && (mqttData.power === 1 || mqttData.m1_green === 1 || mqttData.y2_green === 1)))) {
      state.schedule.enabled = true;
      if (mqttData.start_hour !== undefined && mqttData.start_minute !== undefined) {
        const sH = String(mqttData.start_hour).padStart(2, '0');
        const sM = String(mqttData.start_minute).padStart(2, '0');
        state.schedule.onTime = `${sH}:${sM}`;
        if (DOM.onTime) {
          DOM.onTime.value = state.schedule.onTime;
          DOM.onTime.disabled = true;
        }
        if (mqttData.start_year && mqttData.start_month && mqttData.start_day) {
          const sY = String(mqttData.start_year);
          const sMo = String(mqttData.start_month).padStart(2, '0');
          const sD = String(mqttData.start_day).padStart(2, '0');
          state.schedule.onDate = `${sY}-${sMo}-${sD}`;
          if (DOM.onDate) {
            DOM.onDate.value = state.schedule.onDate;
            DOM.onDate.disabled = true;
          }
        }
      }
      if (mqttData.stop_hour !== undefined && mqttData.stop_minute !== undefined && (mqttData.stop_hour > 0 || mqttData.stop_minute > 0)) {
        const eH = String(mqttData.stop_hour).padStart(2, '0');
        const eM = String(mqttData.stop_minute).padStart(2, '0');
        state.schedule.offTime = `${eH}:${eM}`;
        if (DOM.offTime) {
          DOM.offTime.value = state.schedule.offTime;
          DOM.offTime.disabled = true;
        }
        if (mqttData.stop_year && mqttData.stop_year >= 2020 && mqttData.stop_month && mqttData.stop_day) {
          const eY = String(mqttData.stop_year);
          const eMo = String(mqttData.stop_month).padStart(2, '0');
          const eD = String(mqttData.stop_day).padStart(2, '0');
          state.schedule.offDate = `${eY}-${eMo}-${eD}`;
          if (DOM.offDate) {
            DOM.offDate.value = state.schedule.offDate;
            DOM.offDate.disabled = true;
          }
        }
      }
      state.acOn = true;
      state.acPower = 1;
      updateSystemState('running');
      updateControlButtons();
      updateScheduleSummary();
      saveSettings();
    }

    // Real-Time Machine Running & Lamp Status from PLC (Source of Truth: M2, M12, Y1, Y2)
    const isPlcRed = (mqttData.m2_red === 1 || mqttData.m12_red === 1 || mqttData.red_lamp === 1 || mqttData.y0_red === 1);
    const isPlcRunning = (mqttData.power === 1 || mqttData.y2_green === 1 || mqttData.m1_green === 1);
    const isPlcYellow = (mqttData.y1_yellow === 1 || mqttData.m3_yellow === 1);

    if (state.scheduleMode === 'none') {
      state.acOn = false;
      state.acPower = 0;
      if (state.systemState !== 'idle') {
        updateSystemState('idle');
      }
    } else if (isPlcRed) {
      // Hardware Red Lamp is Active on PLC (M2/M12 timeout / stop lock)
      if (state.systemState !== 'timeout') {
        updateSystemState('timeout');
      }
      state.acOn = false;
      state.acPower = 0;
    } else if (isPlcRunning) {
      // Machine is Running (Green Light Active)
      if (state.systemState !== 'running') {
        updateSystemState('running');
      }
      state.acOn = true;
      state.acPower = 1;
    } else {
      // AC is OFF and Hardware Red Lamp is NOT Active (M2=0, M12=0)
      if (now < state.userActionUntil && (state.systemState === 'stopped' || state.systemState === 'timeout')) {
        // Keep optimistic stop/timeout during user action window (5 seconds)
        state.acOn = false;
        state.acPower = 0;
      } else if (state.scheduleMode === 'manual') {
        if (!state.schedule.enabled) {
          // Manual mode without schedule saved -> Step 1: IDLE (Yellow Blink)
          state.acOn = false;
          state.acPower = 0;
          if (state.systemState !== 'idle') {
            updateSystemState('idle');
          }
        } else {
          // Manual mode with schedule saved -> check if expired or waiting
          const onD = state.schedule.onDate || getTodayIso();
          const offD = state.schedule.offDate || getTodayIso();
          const stopDt = parseScheduleDateTime(offD, state.schedule.offTime);
          if (stopDt && new Date() >= stopDt) {
            if (state.systemState !== 'timeout') {
              updateSystemState('timeout');
            }
          } else {
            if (state.systemState !== 'ready') {
              updateSystemState('ready');
            }
          }
          state.acOn = false;
          state.acPower = 0;
        }
      } else if (state.scheduleMode === 'auto') {
        const nowDate = new Date();
        const curMins = nowDate.getHours() * 60 + nowDate.getMinutes();
        const isInAutoTime = (curMins >= 8 * 60 && curMins < 17 * 60);
        if (isInAutoTime) {
          // เมื่อถึงเวลา (08:00 - 17:00) -> เขียวค้าง (RUNNING)
          if (state.systemState !== 'running') {
            updateSystemState('running');
          }
          state.acOn = true;
          state.acPower = 1;
        } else {
          // นอกเวลา -> นอกเวลาเขียวจะกระพริบ (READY)
          if (state.systemState !== 'ready') {
            updateSystemState('ready');
          }
          state.acOn = false;
          state.acPower = 0;
        }
      }
    }

    // In NONE mode, keep inputs blank / empty as requested by user
    if (state.scheduleMode !== 'none') {
      if (mqttData.power !== undefined && now > state.userModifiedPowerUntil && !state.userModifiedPower) {
        state.acPower = Number(mqttData.power);
        state.acOn = (state.acPower === 1);
        updateControlButtons();
      }

      if (mqttData.temperature !== undefined && now > state.userModifiedTempUntil && !state.userModifiedTemp) {
        const t = parseFloat(mqttData.temperature);
        if (!isNaN(t) && t >= 18 && t <= 27) {
          state.targetTemp = t;
          if (DOM.targetTemp && document.activeElement !== DOM.targetTemp) {
            DOM.targetTemp.value = t;
          }
          document.querySelectorAll('.temp-chip').forEach((chip) => {
            const chipVal = parseFloat(chip.getAttribute('data-temp'));
            chip.classList.toggle('temp-chip--active', chipVal === t);
          });
          updateMqttTempDisplay();
        }
      }

      if (mqttData.mode !== undefined && now > state.userModifiedModeUntil && !state.userModifiedMode) {
        state.acMode = Number(mqttData.mode);
        if (DOM.modeSelect && document.activeElement !== DOM.modeSelect) {
          DOM.modeSelect.value = state.acMode;
        }
      }

      if (mqttData.fan !== undefined && now > state.userModifiedFanUntil && !state.userModifiedFan) {
        state.acFan = Number(mqttData.fan);
        if (DOM.fanSelect && document.activeElement !== DOM.fanSelect) {
          DOM.fanSelect.value = state.acFan;
        }
      }
    }

    // Hardware Status Badges
    if (mqttData.esp32_online !== undefined) {
      state.esp32Online = Boolean(mqttData.esp32_online);
    }
    if (mqttData.plc_online !== undefined) {
      state.plcOnline = Boolean(mqttData.plc_online);
    }

    // PLC Real-Time Clock TRD D400-D406 Readback (ซิงค์เวลาจริงจาก PLC)
    const plcMin = (mqttData.plc_rtc_min !== undefined) ? mqttData.plc_rtc_min : mqttData.plc_rtc_minute;
    if (mqttData.plc_rtc_hour !== undefined && plcMin !== undefined) {
      state.plcRtc = {
        year: Number(mqttData.plc_rtc_year || new Date().getFullYear()),
        month: Number(mqttData.plc_rtc_month || (new Date().getMonth() + 1)),
        day: Number(mqttData.plc_rtc_day || new Date().getDate()),
        hour: Number(mqttData.plc_rtc_hour),
        minute: Number(plcMin),
        second: Number(mqttData.plc_rtc_sec || 0),
        dayOfWeek: Number(mqttData.plc_rtc_dow || 0),
        timeStr: mqttData.plc_rtc_time || '',
        valid: true,
        lastSync: Date.now(),
      };
    }

    // 3 Temperature Sensors Readback (D0-D2)
    if (mqttData.temp1 !== undefined) updateSensor(1, parseFloat(mqttData.temp1));
    if (mqttData.temp2 !== undefined) updateSensor(2, parseFloat(mqttData.temp2));
    if (mqttData.temp3 !== undefined) updateSensor(3, parseFloat(mqttData.temp3));

    // Light Sensor Readback from PLC Register D10 (TSL2591)
    const d10Val = (mqttData.d10 !== undefined) ? mqttData.d10 :
                   (mqttData.d10_lux !== undefined) ? mqttData.d10_lux :
                   (mqttData.lux !== undefined) ? mqttData.lux :
                   (mqttData.d10_rs485 !== undefined) ? mqttData.d10_rs485 : undefined;
    if (d10Val !== undefined) {
      updateLuxSensor(parseFloat(d10Val));
    }

    if (mqttData.temp1 !== undefined || mqttData.temp2 !== undefined || mqttData.temp3 !== undefined || d10Val !== undefined) {
      updateTempBadge();
      if (typeof SensorHistoryManager !== 'undefined' && SensorHistoryManager.onTelemetry) {
        SensorHistoryManager.onTelemetry();
      }
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

  // ── Unified Schedule & Live Duration Summary ──
  function updateScheduleSummary() {
    if (!DOM.summaryModeText || !DOM.summaryTimeText || !DOM.summaryDurationText || !DOM.summaryProgressText) return;

    const mode = state.scheduleMode;
    const isNone = (mode === 'none');
    const isAuto = (mode === 'auto');
    const isManual = (mode === 'manual');

    // 1. Mode Label
    if (isNone) {
      DOM.summaryModeText.textContent = '⚡ โหมด NONE (สแตนด์บาย)';
      DOM.summaryTimeText.textContent = '--:-- ถึง --:--';
      DOM.summaryDurationText.textContent = '--';
      DOM.summaryProgressText.textContent = '🟡 สแตนด์บาย (รอเลือกโหมด)';
      return;
    }

    if (isAuto) {
      DOM.summaryModeText.textContent = '🔄 โหมด AUTO (ทุกวัน 08:00 - 17:00)';
      DOM.summaryTimeText.textContent = '08:00 ถึง 17:00 (ฟิกซ์ตาม PLC)';
      DOM.summaryDurationText.textContent = '9 ชั่วโมง 00 นาที';

      const now = new Date();
      const todayIso = getTodayIso();
      const { start, stop } = getScheduleRange(todayIso, '08:00', todayIso, '17:00');

      if (start && stop) {
        if (now >= start && now < stop) {
          const remainMs = stop.getTime() - now.getTime();
          const remH = Math.floor(remainMs / (1000 * 60 * 60));
          const remM = Math.floor((remainMs % (1000 * 60 * 60)) / (1000 * 60));
          DOM.summaryProgressText.textContent = `🟢 กำลังทำงาน (เหลือเวลาอีก ${remH} ชม. ${remM} นาที)`;
        } else if (now < start) {
          const waitMs = start.getTime() - now.getTime();
          const waitH = Math.floor(waitMs / (1000 * 60 * 60));
          const waitM = Math.floor((waitMs % (1000 * 60 * 60)) / (1000 * 60));
          DOM.summaryProgressText.textContent = `⚡ รอเริ่มทำงานเวลา 08:00 (อีก ${waitH} ชม. ${waitM} นาที)`;
        } else {
          DOM.summaryProgressText.textContent = '🔴 ครบเวลาการทำงานของวันนี้แล้ว (17:00)';
        }
      }
      return;
    }

    // Manual Mode
    const onT = state.schedule.onTime || DOM.onTime?.value || '';
    const onD = state.schedule.onDate || DOM.onDate?.value || '';
    const offT = state.schedule.offTime || DOM.offTime?.value || '';
    const offD = state.schedule.offDate || DOM.offDate?.value || '';

    DOM.summaryModeText.textContent = '🛠️ โหมด MANUAL (กำหนดเอง)';

    if (!onT || !offT) {
      DOM.summaryTimeText.textContent = 'รอระบุเวลาเริ่ม - หยุด';
      DOM.summaryDurationText.textContent = 'รอตั้งเวลา';
      DOM.summaryProgressText.textContent = '🟡 รอตั้งเวลาและกดบันทึก';
      return;
    }

    const start = parseScheduleDateTime(onD || getTodayIso(), onT);
    const stop = parseScheduleDateTime(offD || getTodayIso(), offT);

    const onDateDisplay = onD ? formatDisplayDate(onD) : getTodayDDMMYYYY();
    const offDateDisplay = offD ? formatDisplayDate(offD) : getTodayDDMMYYYY();
    DOM.summaryTimeText.textContent = `${onDateDisplay} ${onT} ถึง ${offDateDisplay} ${offT}`;

    if (start && stop && stop > start) {
      const diffMs = stop.getTime() - start.getTime();
      const hours = Math.floor(diffMs / (1000 * 60 * 60));
      const mins = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
      DOM.summaryDurationText.textContent = `${hours} ชั่วโมง ${String(mins).padStart(2, '0')} นาที`;

      const now = new Date();
      if (state.systemState === 'stopped' || state.systemState === 'timeout' || now >= stop) {
        DOM.summaryProgressText.textContent = '🔴 หมดเวลาทำงานแล้ว (ไฟแดงติดกระพริบ — กดรีเซทเพื่อเริ่มรอบใหม่)';
      } else if (now >= start && now < stop) {
        const remainMs = stop.getTime() - now.getTime();
        const remH = Math.floor(remainMs / (1000 * 60 * 60));
        const remM = Math.floor((remainMs % (1000 * 60 * 60)) / (1000 * 60));
        DOM.summaryProgressText.textContent = `🟢 กำลังทำงาน (เหลือเวลาอีก ${remH} ชม. ${remM} นาที)`;
      } else if (now < start) {
        const waitMs = start.getTime() - now.getTime();
        const waitH = Math.floor(waitMs / (1000 * 60 * 60));
        const waitM = Math.floor((waitMs % (1000 * 60 * 60)) / (1000 * 60));
        DOM.summaryProgressText.textContent = `⚡ พร้อมทำงาน (รอเริ่มในอีก ${waitH} ชม. ${waitM} นาที)`;
      }
    } else {
      DOM.summaryDurationText.textContent = 'ระบุเวลาไม่ถูกต้อง';
      DOM.summaryProgressText.textContent = '⚠️ เวลาปิดต้องมากกว่าเวลาเปิด';
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
      return; // ห้ามใส่ค่าจำลองหลอก — รอรับค่าจริงจาก ESP32 / PLC ผ่าน MQTT
    }

    const temp = parseFloat(Number(value).toFixed(1));

    // กรองค่าตกฮวบชั่วคราว (เช่น 0.0°C จากสายเซนเซอร์หลวม/PLC Analog Scan หลุด/ยังไม่พร้อม) ให้คงค่าเดิมไว้ไม่ให้ตัวเลขกระพริบ
    if (temp <= 0.0) {
      if (state.sensors[`temp${index}`] != null && state.sensors[`temp${index}`] > 0) {
        return; // คงค่าเดิมที่ถูกต้องไว้
      }
      return; // หากเริ่มต้นยังไม่มีค่า ให้รอค่าจริงที่ > 0
    }

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

  // ── Light Intensity Sensor (TSL2591 / PLC Register D10) ──
  function updateLuxSensor(value) {
    const luxEl = DOM.sensorLuxVal;
    const progressEl = DOM.sensorProgressLux;
    const cardEl = DOM.sensorCardLux;
    if (!luxEl || !cardEl) return;

    if (value == null || isNaN(value)) {
      return; // ห้ามใส่ค่าจำลองหลอก — รอรับค่าจริงจาก ESP32 / PLC ผ่าน MQTT
    }

    const lux = Math.max(0, Math.round(Number(value)));
    state.sensors.lux = lux;
    state.sensors.d10 = lux;

    luxEl.textContent = lux.toLocaleString();

    if (progressEl) {
      // Progress ring scale: 0 - 25,000 Lux (TSL2591 Digital Lux Sensor)
      const maxScale = 25000;
      const pct = Math.min(Math.max(lux / maxScale, 0), 1);
      progressEl.style.strokeDashoffset = String(SENSOR_RING_CIRCUMFERENCE * (1 - pct));
    }

    cardEl.classList.remove('sensor-card--lux-dim', 'sensor-card--lux-normal', 'sensor-card--lux-bright', 'sensor-card--offline');
    if (lux < 50) {
      cardEl.classList.add('sensor-card--lux-dim');
    } else if (lux < 500) {
      cardEl.classList.add('sensor-card--lux-normal');
    } else {
      cardEl.classList.add('sensor-card--lux-bright');
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
    if (state.scheduleMode === 'none') {
      nextState = 'idle';
    } else if (state.scheduleMode === 'manual' && !state.schedule.enabled) {
      if (nextState !== 'stopped' && nextState !== 'timeout') {
        nextState = 'idle';
      }
    }
    state.systemState = nextState;

    setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ');
    setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ');
    setLight(DOM.lightRed, DOM.stateRed, null, 'OFF', '❌ ดับ');

    [DOM.flowIdle, DOM.flowReady, DOM.flowRunning, DOM.flowStopped].forEach((el) => {
      el?.classList.remove('state-flow__step--active');
    });

    if (DOM.scheduleStatusTag) {
      if (nextState === 'timeout' || nextState === 'stopped') {
        DOM.scheduleStatusTag.textContent = '⛔ Step 4: หมดเวลาทำงาน (ไฟแดงติดกระพริบ 1s — ต้องกด "รีเซท" เท่านั้น)';
        DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--pending';
      } else if (nextState === 'running') {
        const onT = state.schedule.onTime || DOM.onTime?.value || '';
        const offT = state.schedule.offTime || DOM.offTime?.value || '';
        if (!onT || !offT || (state.scheduleMode === 'manual' && !state.schedule.enabled)) {
          DOM.scheduleStatusTag.textContent = '⚠️ Step 1: IDLE (สแตนด์บาย / รอตั้งเวลาและกดบันทึกค่า)';
          DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--pending';
        } else {
          DOM.scheduleStatusTag.textContent = `🟢 กำลังทำงาน (${onT} - ${offT})`;
          DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--active';
        }
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
        const readyDot = DOM.flowReady?.querySelector('.state-flow__dot');
        if (readyDot) readyDot.className = 'state-flow__dot state-flow__dot--green-blink';
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
        const runningDot = DOM.flowRunning?.querySelector('.state-flow__dot');
        if (runningDot) runningDot.className = 'state-flow__dot state-flow__dot--green';
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'Step 3: RUNNING (กำลังทำงาน)';
          DOM.currentStateBadge.className = 'state-badge state-badge--running';
        }
        break;

      case 'timeout':
      case 'stopped':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightRed, DOM.stateRed, 'red-blink', 'TIMEOUT (หมดเวลา)', '⚠️ หมดเวลาทำงาน (ไฟแดงติดกระพริบ — ต้องกดรีเซท)');
        DOM.flowStopped?.classList.add('state-flow__step--active');
        const timeoutDot = DOM.flowStopped?.querySelector('.state-flow__dot');
        if (timeoutDot) timeoutDot.className = 'state-flow__dot state-flow__dot--red-blink';
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'Step 4: TIMEOUT (หมดเวลาทำงาน — ไฟแดงติดกระพริบ)';
          DOM.currentStateBadge.className = 'state-badge state-badge--stopped';
        }
        break;

      case 'idle':
      default:
        state.systemState = 'idle';
        DOM.flowIdle?.classList.add('state-flow__step--active');
        const idleDot = DOM.flowIdle?.querySelector('.state-flow__dot');

        if (state.scheduleMode === 'none') {
          // ในโหมด NONE: ไฟเหลืองสแตนด์บายติดค้างดวงเดียวเท่านั้น ไฟเขียวและไฟแดงดับสนิท
          setLight(DOM.lightYellow, DOM.stateYellow, 'amber-solid', 'STANDBY', '🟡 โหมด NONE: สแตนด์บาย');
          setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ');
          setLight(DOM.lightRed, DOM.stateRed, null, 'OFF', '❌ ดับ');
          if (idleDot) idleDot.className = 'state-flow__dot state-flow__dot--amber';
          if (DOM.currentStateBadge) {
            DOM.currentStateBadge.textContent = 'โหมด NONE (สแตนด์บาย)';
            DOM.currentStateBadge.className = 'state-badge state-badge--amber';
          }
        } else if (state.scheduleMode === 'manual') {
          // ในโหมด MANUAL: ไฟเหลืองกระพริบ (รอตั้งเวลาและกดบันทึกค่า)
          setLight(DOM.lightYellow, DOM.stateYellow, 'amber-blink', 'MANUAL (รอตั้งเวลา)', '🟡 MANUAL: ไฟเหลืองกระพริบ (รอตั้งเวลา)');
          setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ');
          setLight(DOM.lightRed, DOM.stateRed, null, 'OFF', '❌ ดับ');
          if (idleDot) idleDot.className = 'state-flow__dot state-flow__dot--amber-blink';
          if (DOM.currentStateBadge) {
            DOM.currentStateBadge.textContent = 'Step 1: IDLE (โหมด MANUAL / ไฟเหลืองกระพริบ)';
            DOM.currentStateBadge.className = 'state-badge state-badge--amber';
          }
        } else {
          // ในโหมด AUTO ที่ยังไม่ได้เริ่ม
          setLight(DOM.lightYellow, DOM.stateYellow, 'amber-solid', 'IDLE', 'IDLE: สแตนด์บาย / รอตั้งเวลา');
          setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ');
          setLight(DOM.lightRed, DOM.stateRed, null, 'OFF', '❌ ดับ');
          if (idleDot) idleDot.className = 'state-flow__dot state-flow__dot--amber';
          if (DOM.currentStateBadge) {
            DOM.currentStateBadge.textContent = 'Step 1: IDLE (สแตนด์บาย / รอตั้งเวลา)';
            DOM.currentStateBadge.className = 'state-badge state-badge--amber';
          }
        }
        break;
    }

    updateControlButtons();
  }

  function updateControlButtons() {
    const isNone = (state.scheduleMode === 'none');
    const isAuto = (state.scheduleMode === 'auto');
    const isManual = (state.scheduleMode === 'manual');

    // ตรวจสอบความถูกต้องของการกรอกเวลาในโหมด MANUAL
    const onTimeVal = DOM.onTime?.value || state.schedule.onTime;
    const onDateVal = DOM.onDate?.value || state.schedule.onDate;
    const offTimeVal = DOM.offTime?.value || state.schedule.offTime;
    const offDateVal = DOM.offDate?.value || state.schedule.offDate;
    const hasValidTimes = Boolean(onTimeVal && onDateVal && offTimeVal && offDateVal);

    // ──────────────────────────────────────────────────────────────
    // 1. สถานะ STOPPED หรือ TIMEOUT (ระบบล็อก ต้องกดรีเซทเท่านั้น)
    // ──────────────────────────────────────────────────────────────
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      if (DOM.btnSave) {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = 'ระบบล็อกอยู่';
      }
      if (DOM.btnStart) {
        DOM.btnStart.disabled = true;
        if (DOM.btnStartHint) DOM.btnStartHint.textContent = 'ระบบถูกล็อก (กดรีเซท)';
      }
      if (DOM.btnStop) {
        DOM.btnStop.disabled = true;
        if (DOM.btnStopHint) DOM.btnStopHint.textContent = 'หยุดทำงานแล้ว';
      }
      if (DOM.btnReset) {
        DOM.btnReset.disabled = false;
        if (DOM.btnResetHint) DOM.btnResetHint.textContent = 'กดเพื่อปลดล็อก';
      }

      // Lock all controls, mode toggles, and inputs during TIMEOUT / STOPPED state
      if (DOM.onDate) DOM.onDate.disabled = true;
      if (DOM.onTime) DOM.onTime.disabled = true;
      if (DOM.offDate) DOM.offDate.disabled = true;
      if (DOM.offTime) DOM.offTime.disabled = true;
      if (DOM.targetTemp) DOM.targetTemp.disabled = true;
      if (DOM.tempMinusBtn) DOM.tempMinusBtn.disabled = true;
      if (DOM.tempPlusBtn) DOM.tempPlusBtn.disabled = true;
      if (DOM.modeSelect) DOM.modeSelect.disabled = true;
      if (DOM.fanSelect) DOM.fanSelect.disabled = true;
      if (DOM.btnSendMqtt) DOM.btnSendMqtt.disabled = true;
      document.querySelectorAll('.temp-chip').forEach(chip => chip.disabled = true);
      if (DOM.modeNoneBtn) DOM.modeNoneBtn.disabled = true;
      if (DOM.modeAutoBtn) DOM.modeAutoBtn.disabled = true;
      if (DOM.modeManualBtn) DOM.modeManualBtn.disabled = true;
      return;
    }

    // ──────────────────────────────────────────────────────────────
    // 2. โหมด NONE: ล็อกปุ่มและอินพุตทุกอย่าง ยกเว้นปุ่มสลับโหมด!
    // ──────────────────────────────────────────────────────────────
    if (isNone) {
      if (DOM.onDate) DOM.onDate.disabled = true;
      if (DOM.onTime) DOM.onTime.disabled = true;
      if (DOM.offDate) DOM.offDate.disabled = true;
      if (DOM.offTime) DOM.offTime.disabled = true;

      if (DOM.targetTemp) DOM.targetTemp.disabled = true;
      if (DOM.tempMinusBtn) DOM.tempMinusBtn.disabled = true;
      if (DOM.tempPlusBtn) DOM.tempPlusBtn.disabled = true;
      if (DOM.modeSelect) DOM.modeSelect.disabled = true;
      if (DOM.fanSelect) DOM.fanSelect.disabled = true;
      if (DOM.btnSendMqtt) DOM.btnSendMqtt.disabled = true;
      document.querySelectorAll('.temp-chip').forEach(chip => chip.disabled = true);

      if (DOM.btnSave) {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = 'เลือกโหมดเพื่อเริ่ม';
        DOM.btnSave.title = 'โหมด NONE ไม่สามารถใช้งานได้';
      }
      if (DOM.btnStart) {
        DOM.btnStart.disabled = true;
        if (DOM.btnStartHint) DOM.btnStartHint.textContent = 'เลือกโหมดเพื่อเริ่ม';
        DOM.btnStart.title = 'โหมด NONE ไม่สามารถใช้งานได้';
      }
      if (DOM.btnStop) {
        DOM.btnStop.disabled = true;
        if (DOM.btnStopHint) DOM.btnStopHint.textContent = 'เลือกโหมดเพื่อเริ่ม';
        DOM.btnStop.title = 'โหมด NONE ไม่สามารถใช้งานได้';
      }
      if (DOM.btnReset) {
        DOM.btnReset.disabled = false;
        if (DOM.btnResetHint) DOM.btnResetHint.textContent = 'กดเพื่อรีเซท';
        DOM.btnReset.title = 'กดเพื่อรีเซทระบบและปิดเครื่องปรับอากาศ';
      }

      // ปลดล็อกเฉพาะปุ่มสลับโหมด (NONE / AUTO / MANUAL)
      if (DOM.modeNoneBtn) DOM.modeNoneBtn.disabled = false;
      if (DOM.modeAutoBtn) DOM.modeAutoBtn.disabled = false;
      if (DOM.modeManualBtn) DOM.modeManualBtn.disabled = false;
      return;
    }

    // ──────────────────────────────────────────────────────────────
    // 3. โหมด AUTO: เวลาฟิกซ์ 08:00 - 17:00 (ปรับอุณหภูมิได้)
    // ──────────────────────────────────────────────────────────────
    if (isAuto) {
      if (DOM.onDate) DOM.onDate.disabled = true;
      if (DOM.onTime) DOM.onTime.disabled = true;
      if (DOM.offDate) DOM.offDate.disabled = true;
      if (DOM.offTime) DOM.offTime.disabled = true;

      // เปิดให้ปรับอุณหภูมิและส่งคำสั่งเครื่องปรับอากาศได้
      if (DOM.targetTemp) DOM.targetTemp.disabled = false;
      if (DOM.tempMinusBtn) DOM.tempMinusBtn.disabled = false;
      if (DOM.tempPlusBtn) DOM.tempPlusBtn.disabled = false;
      if (DOM.modeSelect) DOM.modeSelect.disabled = false;
      if (DOM.fanSelect) DOM.fanSelect.disabled = false;
      if (DOM.btnSendMqtt) DOM.btnSendMqtt.disabled = false;
      document.querySelectorAll('.temp-chip').forEach(chip => chip.disabled = false);

      // ล็อกปุ่มบันทึกและเริ่ม เพราะ AUTO ทำงานตามเวลาฟิกซ์
      if (DOM.btnSave) {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = 'เวลาฟิกซ์ 08:00-17:00';
        DOM.btnSave.title = 'โหมด AUTO ฟิกซ์เวลาอัตโนมัติ';
      }
      if (DOM.btnStart) {
        DOM.btnStart.disabled = true;
        if (DOM.btnStartHint) DOM.btnStartHint.textContent = (state.systemState === 'running') ? 'กำลังทำงานอัตโนมัติ' : 'ทำงานตามเวลา 08:00';
        DOM.btnStart.title = 'โหมด AUTO ทำงานอัตโนมัติ';
      }
      if (DOM.btnStop) {
        DOM.btnStop.disabled = (state.systemState !== 'running');
        if (DOM.btnStopHint) {
          DOM.btnStopHint.textContent = (state.systemState === 'running') ? 'กดเพื่อหยุดทำงาน' : 'กดเมื่อเริ่มทำงาน';
        }
        DOM.btnStop.title = (state.systemState === 'running') ? 'กดเพื่อหยุดการทำงาน (OFF)' : 'สามารถกดหยุดได้เมื่อถึงเวลาทำงาน (08:00 - 17:00)';
      }
      if (DOM.btnReset) {
        DOM.btnReset.disabled = false;
        if (DOM.btnResetHint) DOM.btnResetHint.textContent = 'กดกลับสู่ NONE';
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
    if (DOM.modeSelect) DOM.modeSelect.disabled = false;
    if (DOM.fanSelect) DOM.fanSelect.disabled = false;
    if (DOM.btnSendMqtt) DOM.btnSendMqtt.disabled = false;
    document.querySelectorAll('.temp-chip').forEach(chip => chip.disabled = false);

    if (DOM.modeNoneBtn) DOM.modeNoneBtn.disabled = false;
    if (DOM.modeAutoBtn) DOM.modeAutoBtn.disabled = false;
    if (DOM.modeManualBtn) DOM.modeManualBtn.disabled = false;

    // การล็อกเวลา: เมื่อบันทึกค่าแล้ว (state.schedule.enabled == true) จะล็อกทันทีจนกว่าจะกดรีเซท
    const isTimeLocked = state.schedule.enabled;
    if (DOM.onDate) DOM.onDate.disabled = isTimeLocked;
    if (DOM.onTime) DOM.onTime.disabled = isTimeLocked;
    if (DOM.offDate) DOM.offDate.disabled = isTimeLocked;
    if (DOM.offTime) DOM.offTime.disabled = isTimeLocked;

    // ขั้นที่ 1 & 2: ปุ่มบันทึกค่า (btnSave)
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
      DOM.btnSave.title = isTimeLocked ? 'บันทึกเวลาเรียบร้อยแล้ว (กดรีเซทเพื่อเปลี่ยนค่าใหม่)' : (hasValidTimes ? 'กดเพื่อบันทึกค่า' : 'กรุณาตั้งเวลาเปิด-ปิดให้ครบ');
    }

    // ขั้นที่ 3: ปุ่มเริ่มทำงาน (btnStart)
    const now = new Date();
    const onD = state.schedule.onDate || DOM.onDate?.value || getTodayIso();
    const onT = state.schedule.onTime || DOM.onTime?.value;
    const startDt = (onD && onT) ? parseScheduleDateTime(onD, onT) : null;
    const isBeforeStart = Boolean(startDt && now < startDt);

    const canStart = state.schedule.enabled && (state.systemState !== 'running') && !isBeforeStart;
    if (DOM.btnStart) {
      DOM.btnStart.disabled = !canStart;
      if (DOM.btnStartHint) {
        if (state.systemState === 'running') {
          DOM.btnStartHint.textContent = 'กำลังทำงาน';
        } else if (!state.schedule.enabled) {
          DOM.btnStartHint.textContent = 'ต้องกดบันทึกค่าก่อน';
        } else if (isBeforeStart) {
          DOM.btnStartHint.textContent = `รอเวลาเริ่ม (${onT})`;
        } else {
          DOM.btnStartHint.textContent = 'กดเพื่อเริ่มทำงาน';
        }
      }
      DOM.btnStart.title = (state.systemState === 'running') ? 'เครื่องปรับอากาศกำลังทำงาน' :
        (!state.schedule.enabled) ? 'กรุณากดบันทึกค่าก่อน' :
        (isBeforeStart) ? `ยังไม่ถึงเวลาเริ่มทำงาน (${onT}) — ระบบจะเริ่มทำงานให้อัตโนมัติเมื่อถึงเวลา` :
        'กดเพื่อเริ่มทำงานเครื่องปรับอากาศ';
    }

    // ปุ่มหยุดทำงาน: ปลดล็อกให้กดได้เมื่อเครื่องกำลังรัน (running)
    if (DOM.btnStop) {
      DOM.btnStop.disabled = (state.systemState !== 'running');
      if (DOM.btnStopHint) {
        DOM.btnStopHint.textContent = (state.systemState === 'running') ? 'กดเพื่อหยุดทำงาน' : 'กดเมื่อเริ่มทำงาน';
      }
      DOM.btnStop.title = (state.systemState === 'running') ? 'กดเพื่อหยุดการทำงาน (OFF)' : 'สามารถกดหยุดได้เมื่อเครื่องกำลังทำงาน';
    }

    // ปุ่มรีเซท: สามารถกดรีเซทได้เสมอในโหมด MANUAL
    if (DOM.btnReset) {
      DOM.btnReset.disabled = false;
      if (DOM.btnResetHint) DOM.btnResetHint.textContent = 'กดเพื่อรีเซท';
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

    let onDateVal, onTimeVal, offDateVal, offTimeVal;

    if (isAutoMode) {
      onDateVal = todayIso;
      onTimeVal = '08:00';
      offDateVal = todayIso;
      offTimeVal = '17:00';
    } else {
      onDateVal = DOM.onDate?.value || todayIso;
      onTimeVal = DOM.onTime?.value;
      offDateVal = DOM.offDate?.value || todayIso;
      offTimeVal = DOM.offTime?.value;
    }

    const targetTemp = getValidTargetTemp();

    if (!onTimeVal || !offTimeVal) {
      showToast('error', 'กรุณากำหนดเวลาเปิดและเวลาปิดเครื่องปรับอากาศให้ครบถ้วน');
      return;
    }

    const modeLabel = isAutoMode ? '[AUTO]' : '[MANUAL]';
    const now = new Date();
    const start = parseScheduleDateTime(onDateVal, onTimeVal);
    const stop = parseScheduleDateTime(offDateVal, offTimeVal);

    if (!start || !stop) {
      showToast('error', 'รูปแบบเวลาไม่ถูกต้อง กรุณากำหนดเวลาใหม่');
      return;
    }

    if (!isAutoMode) {
      // กฎที่ 1: เวลาเริ่มต้องมากกว่าเวลาปัจจุบันอย่างน้อย 1 นาที (เตือนกรณีตั้งเวลาน้อยกว่าปัจจุบัน 1min)
      const minStart = new Date(now.getTime() + 60 * 1000);
      if (start.getTime() < minStart.getTime()) {
        showToast('warning', `⚠️ เวลาเริ่มเปิดเครื่องต้องมากกว่าเวลาปัจจุบันอย่างน้อย 1 นาที (ปัจจุบัน ${now.toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit', hour12: false })})`);
        addLog('warning', `[Schedule] ตั้งเวลาไม่ถูกต้อง — เวลาเริ่ม (${onTimeVal}) ต้องมากกว่าเวลาปัจจุบันอย่างน้อย 1 นาที`);
        return;
      }

      // กฎที่ 2: เวลาหยุดขั้นต่ำ 5 นาที (ต้องห่างจากเวลาเริ่มอย่างน้อย 5 นาที)
      const minStop = new Date(start.getTime() + 5 * 60 * 1000);
      if (stop.getTime() < minStop.getTime()) {
        showToast('warning', '⚠️ เวลาหยุดทำงานขั้นต่ำต้องห่างจากเวลาเริ่มอย่างน้อย 5 นาที');
        addLog('warning', `[Schedule] ตั้งเวลาไม่ถูกต้อง — เวลาหยุด (${offTimeVal}) ต้องห่างจากเวลาเริ่ม (${onTimeVal}) อย่างน้อย 5 นาที`);
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

    // ในโหมด MANUAL: ส่งค่า Modbus (D500-D504 Stop Time + M100=ON + M42=ON) เมื่อกดปุ่มบันทึกค่า
    if (!isAutoMode) {
      sendMqttPayload(0, targetTemp, 0, state.acFan, 0, 0, 0, 0, 0, 0, 1, true, 1);
    }

    // บันทึกค่าสำเร็จ -> ตรวจสอบว่าถึงเวลาเริ่มหรือยัง
    if (now >= start && now < stop) {
      state.acOn = true;
      updateSystemState('running');
      sendMqttPayload(1, targetTemp, 0, state.acFan, 0, 0, 0, 0, 1);
      startIrTransmissionLock(5500);
      showToast('success', `${modeLabel} ถึงเวลาเริ่มพอดี — เปิดเครื่องปรับอากาศและยิงสัญญาณ IR (${targetTemp}°C)`);
    } else {
      state.acOn = false;
      updateSystemState('ready');
      addLog('success', `${modeLabel} ตั้งเวลาสำเร็จ: ${formatDisplayDate(onDateVal)} ${onTimeVal} - ${formatDisplayDate(offDateVal)} ${offTimeVal} (${targetTemp}°C)`);
      showToast('success', `${modeLabel} บันทึกเวลาสำเร็จ — รอถึงเวลาเปิด (${onTimeVal}) ระบบจะเริ่มทำงานให้อัตโนมัติ`);
    }

    broadcastUiSync('save_schedule');
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

    let onDateVal, onTimeVal, offDateVal, offTimeVal;

    if (isAutoMode) {
      onDateVal = todayIso;
      onTimeVal = '08:00';
      offDateVal = todayIso;
      offTimeVal = '17:00';
    } else {
      onDateVal = state.schedule.onDate || DOM.onDate?.value || todayIso;
      onTimeVal = state.schedule.onTime || DOM.onTime?.value;
      offDateVal = state.schedule.offDate || DOM.offDate?.value || todayIso;
      offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    }

    const targetTemp = getValidTargetTemp();
    const finalOnDate = onDateVal || todayIso;
    const finalOffDate = offDateVal || todayIso;
    const modeLabel = isAutoMode ? '[AUTO]' : '[MANUAL]';
    const now = new Date();
    const start = parseScheduleDateTime(finalOnDate, onTimeVal);
    const stop = parseScheduleDateTime(finalOffDate, offTimeVal);

    if (!start || !stop) {
      showToast('error', 'รูปแบบเวลาไม่ถูกต้อง กรุณากำหนดเวลาใหม่');
      return;
    }

    // ตรวจสอบเงื่อนไข: ถ้ายังไม่ถึงเวลาเริ่ม จะไม่สามารถกดเริ่มทำงานได้
    if (now < start) {
      showToast('warning', `⏳ ยังไม่ถึงเวลาเริ่มทำงาน (${onTimeVal}) — ไม่สามารถกดเริ่มทำงานได้ (ระบบจะเริ่มให้อัตโนมัติเมื่อถึงเวลา)`);
      return;
    }

    if (now >= stop) {
      showToast('warning', `🔴 เลยเวลาทำงานแล้ว (${offTimeVal}) — กรุณากดปุ่มรีเซทเพื่อตั้งเวลาใหม่`);
      return;
    }

    state.schedule.onDate = finalOnDate;
    state.schedule.onTime = onTimeVal;
    state.schedule.offDate = finalOffDate;
    state.schedule.offTime = offTimeVal;
    state.schedule.enabled = true;

    // กดเริ่มทำงาน -> ส่งคำสั่งเปิดเครื่องปรับอากาศ M5=ON และ D10=1, D11=Temp, ยิง IR 10x
    state.acOn = true;
    sendMqttPayload(1, targetTemp, 0, state.acFan, 0, 0, 0, 0, 1); // start_btn = 1 (Triggers M5 ON & IR 10x)
    startIrTransmissionLock(5500);
    updateSystemState('running');
    broadcastUiSync('start_ac');
    addLog('success', `${modeLabel} กดเริ่มทำงาน — สั่งเปิดเครื่องปรับอากาศสำเร็จ (กำลังยิง IR 10 รอบ...)`);
    showToast('success', `${modeLabel} เริ่มทำงานแล้ว — สั่งเปิดเครื่องปรับอากาศ (${targetTemp}°C)`);
  }

  function stopAC() {
    if (state.scheduleMode === 'none') {
      showToast('warning', 'โหมด NONE ถูกล็อก — กรุณาเลือกโหมด AUTO หรือ MANUAL ก่อน');
      return;
    }
    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      showToast('warning', 'ระบบหยุดทำงานแล้ว (ไฟแดงติดกระพริบ) — ต้องกดปุ่ม "รีเซท" ก่อนเท่านั้น');
      return;
    }
    if (state.systemState !== 'running') {
      showToast('info', 'เครื่องปรับอากาศยังไม่ได้เริ่มทำงาน (สามารถกดหยุดได้เมื่อเครื่องกำลังทำงาน)');
      return;
    }

    state.acOn = false;
    state.acPower = 0;
    state.userActionUntil = Date.now() + 5000;
    state.userModifiedPowerUntil = Date.now() + 5000;
    updateSystemState('stopped');

    // ส่งคำสั่งหยุด (stop_btn = 1) และยิง IR ปิดแอร์ 10 รอบ
    sendMqttPayload(0, getValidTargetTemp(), 0, state.acFan, 0, 0, 0, 1, 0);
    startIrTransmissionLock(5500);

    const modeLabel = (state.scheduleMode === 'auto') ? '[AUTO]' : '[MANUAL]';
    addLog('warning', `${modeLabel} กดปุ่ม STOP — สั่งหยุดการทำงานและยิงสัญญาณ IR ปิดแอร์ (ต้องกดรีเซทเท่านั้น)`);
    showToast('warning', '🛑 หยุดการทำงานของเครื่องปรับอากาศแล้ว (กรุณากดปุ่ม "รีเซท" เพื่อเริ่มรอบใหม่)');
    broadcastUiSync('stop_ac');
    updateControlButtons();
  }

  function resetSystem() {

    state.acOn = false;
    state.schedule.enabled = false;
    state.schedule.onTime = '';
    state.schedule.onDate = '';
    state.schedule.offTime = '';
    state.schedule.offDate = '';

    if (DOM.onTime) DOM.onTime.value = '';
    if (DOM.onDate) DOM.onDate.value = '';
    if (DOM.offTime) DOM.offTime.value = '';
    if (DOM.offDate) DOM.offDate.value = '';

    // เมื่อกดรีเซท ให้สลับเด้งกลับสู่โหมด NONE MODE ทันที (ทั้งจาก AUTO และ MANUAL)
    state.scheduleMode = 'none';

    // ปลดล็อคสถานะระบบและ IR timer
    if (state.irTimer) {
      clearTimeout(state.irTimer);
      state.irTimer = null;
    }
    state.irTransmitting = false;
    state.preStopWarned = false;
    state.systemState = 'idle';

    state.userActionUntil = Date.now() + 5000;
    state.userModifiedModeUntil = Date.now() + 5000;
    state.userModifiedPowerUntil = Date.now() + 5000;

    // ส่งคำสั่ง reset=1 ไปยัง ESP32 เพื่อให้ปลดล็อค M500 (Complete Flag = OFF)
    sendMqttPayload(0, getValidTargetTemp(), 0, state.acFan, 0, 1, 0, 0, 0, 0, 0, false, 0, 1);
    saveSettings();

    // สลับหน้าจอและการควบคุมเข้าสู่ NONE MODE ทันที
    applyScheduleMode('none');

    broadcastUiSync('reset_system');
    addLog('info', 'รีเซทระบบเรียบร้อย — เด้งกลับสู่โหมด NONE (เลือกระบบ AUTO/MANUAL เพื่อเริ่ม)');
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
    state.schedule.onDate = '';
    state.schedule.offTime = '';
    state.schedule.offDate = '';

    if (DOM.onTime) DOM.onTime.value = '';
    if (DOM.onDate) DOM.onDate.value = '';
    if (DOM.offTime) DOM.offTime.value = '';
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

    // การเปลี่ยนแปลงจำลองรอบๆ ค่าจริงของ Hardware เซนเซอร์ (31°C, 49°C, 29°C, 394 Lux)
    const base1 = 31.0 + Math.sin(Date.now() / 5000) * 0.8;
    const base2 = 49.0 + Math.cos(Date.now() / 4000) * 2.5;
    const base3 = 29.0 + Math.sin(Date.now() / 6000) * 0.6;
    const baseLux = Math.round(394 + Math.sin(Date.now() / 4500) * 35);

    const data = {
      power: state.acOn ? 1 : 0,
      temperature: state.targetTemp,
      mode: 0,
      fan: state.acFan,
      temp1: parseFloat(base1.toFixed(1)),
      temp2: parseFloat(base2.toFixed(1)),
      temp3: parseFloat(base3.toFixed(1)),
      lux: baseLux,
      d10_lux: baseLux,
      esp32_online: true,
      plc_online: false,
      modbus_online: false,
      simulation: true,
      machine_state: state.acOn ? 'running' : 'stopped',
    };

    updateSensor(1, parseFloat((base1 + (Math.random() - 0.5) * 0.4).toFixed(1)));
    updateSensor(2, parseFloat((base2 + (Math.random() - 0.5) * 0.6).toFixed(1)));
    updateSensor(3, parseFloat((base3 + (Math.random() - 0.5) * 0.3).toFixed(1)));
    updateLuxSensor(baseLux);
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

  // ============================================================
  //  SENSOR HISTORY MANAGER (30-MINUTE INTERVAL & 30-DAY PURGE)
  //  Independent Telemetry Logger & Visualizer (Read-Only Observer)
  //  Does NOT touch or modify AC control system / PLC registers
  // ============================================================
  const SensorHistoryManager = {
    DB_NAME: 'AirSensorHistoryDB',
    STORE_NAME: 'sensor_records',
    DB_VERSION: 1,
    LOG_INTERVAL_MS: 30 * 60 * 1000, // 30 นาที (1,800,000 ms)
    RETENTION_MS: 30 * 24 * 60 * 60 * 1000, // 30 วัน (2,592,000,000 ms)
    STORAGE_KEY: 'aircon_sensor_history_records',
    LAST_LOG_KEY: 'aircon_sensor_last_log_time',

    db: null,
    records: [],
    lastLogTime: 0,
    currentRange: '24h',
    customStart: '',
    customEnd: '',
    activeSensors: { s1: true, s2: true, s3: true, lux: true },
    chartMode: 'all', // 'all' | 'temp' | 'lux'
    pageSize: 25,
    currentPage: 1,
    searchQuery: '',
    hoverIndex: -1,
    tickerTimer: null,
    canvas: null,
    ctx: null,
    liveSamples: {
      temp1: [],
      temp2: [],
      temp3: [],
      lux: [],
      maxSamples: 2000,
    },

    async init() {
      const savedLast = localStorage.getItem(this.LAST_LOG_KEY);
      this.lastLogTime = savedLast ? parseInt(savedLast, 10) : 0;

      await this.initDB();
      await this.loadRecords();
      await this.pruneExpiredRecords();
      await this.sanitizeDummyRecords();

      if (this.records.length === 0) {
        this.lastLogTime = 0;
        try { localStorage.setItem(this.LAST_LOG_KEY, '0'); } catch (e) {}
      } else {
        this.lastLogTime = Math.max(this.lastLogTime, this.records[this.records.length - 1].timestamp);
      }

      this.bindUI();
      this.startTicker();
      this.render();
      this.updateCloudSyncStatus('synced', 'พร้อมซิงค์คลาวด์');
    },

    initDB() {
      return new Promise((resolve) => {
        if (!window.indexedDB) {
          resolve();
          return;
        }
        try {
          const req = window.indexedDB.open(this.DB_NAME, this.DB_VERSION);
          req.onupgradeneeded = (e) => {
            const db = e.target.result;
            if (!db.objectStoreNames.contains(this.STORE_NAME)) {
              const store = db.createObjectStore(this.STORE_NAME, { keyPath: 'timestamp' });
              store.createIndex('idx_timestamp', 'timestamp', { unique: true });
            }
          };
          req.onsuccess = (e) => {
            this.db = e.target.result;
            resolve();
          };
          req.onerror = () => resolve();
        } catch (e) {
          resolve();
        }
      });
    },

    loadRecords() {
      return new Promise((resolve) => {
        if (this.db) {
          try {
            const tx = this.db.transaction([this.STORE_NAME], 'readonly');
            const store = tx.objectStore(this.STORE_NAME);
            const req = store.getAll();
            req.onsuccess = () => {
              this.records = req.result || [];
              this.records.sort((a, b) => a.timestamp - b.timestamp);
              resolve();
            };
            req.onerror = () => {
              this.loadFallback();
              resolve();
            };
          } catch (e) {
            this.loadFallback();
            resolve();
          }
        } else {
          this.loadFallback();
          resolve();
        }
      });
    },

    loadFallback() {
      try {
        const raw = localStorage.getItem(this.STORAGE_KEY);
        this.records = raw ? JSON.parse(raw) : [];
        this.records.sort((a, b) => a.timestamp - b.timestamp);
      } catch (e) {
        this.records = [];
      }
    },

    saveRecord(record) {
      return new Promise((resolve) => {
        const exists = this.records.some(r => r.timestamp === record.timestamp);
        if (!exists) {
          this.records.push(record);
          this.records.sort((a, b) => a.timestamp - b.timestamp);
        }

        const afterSave = () => {
          this.syncFallback();
          this.publishCloudHistory();
          this.broadcastNewRecord(record);
          resolve();
        };

        if (this.db) {
          try {
            const tx = this.db.transaction([this.STORE_NAME], 'readwrite');
            const store = tx.objectStore(this.STORE_NAME);
            store.put(record);
            tx.oncomplete = afterSave;
            tx.onerror = afterSave;
          } catch (e) {
            afterSave();
          }
        } else {
          afterSave();
        }
      });
    },

    syncFallback() {
      try {
        const slice = this.records.slice(-1500);
        localStorage.setItem(this.STORAGE_KEY, JSON.stringify(slice));
      } catch (e) {
        // quota handled safely
      }
    },

    async pruneExpiredRecords() {
      const now = Date.now();
      const cutoff = now - this.RETENTION_MS;
      const initial = this.records.length;
      this.records = this.records.filter((r) => r.timestamp >= cutoff);
      const pruned = initial - this.records.length;

      if (this.db && pruned > 0) {
        try {
          const tx = this.db.transaction([this.STORE_NAME], 'readwrite');
          const store = tx.objectStore(this.STORE_NAME);
          const range = IDBKeyRange.upperBound(cutoff, true);
          store.delete(range);
        } catch (e) {
          // ignore
        }
      }
      if (pruned > 0) {
        this.syncFallback();
      }
    },

    getCurrentSensorValues() {
      // ดึงค่าจริงจากเซนเซอร์ Real-time Telemetry (state.sensors จาก MQTT)
      let t1 = (state.sensors.temp1 != null && !isNaN(state.sensors.temp1))
        ? parseFloat(Number(state.sensors.temp1).toFixed(1))
        : (DOM.sensorTemp1 && !isNaN(parseFloat(DOM.sensorTemp1.textContent)) && DOM.sensorTemp1.textContent !== '--.-')
        ? parseFloat(DOM.sensorTemp1.textContent)
        : null;

      let t2 = (state.sensors.temp2 != null && !isNaN(state.sensors.temp2))
        ? parseFloat(Number(state.sensors.temp2).toFixed(1))
        : (DOM.sensorTemp2 && !isNaN(parseFloat(DOM.sensorTemp2.textContent)) && DOM.sensorTemp2.textContent !== '--.-')
        ? parseFloat(DOM.sensorTemp2.textContent)
        : null;

      let t3 = (state.sensors.temp3 != null && !isNaN(state.sensors.temp3))
        ? parseFloat(Number(state.sensors.temp3).toFixed(1))
        : (DOM.sensorTemp3 && !isNaN(parseFloat(DOM.sensorTemp3.textContent)) && DOM.sensorTemp3.textContent !== '--.-')
        ? parseFloat(DOM.sensorTemp3.textContent)
        : null;

      let lux = (state.sensors.lux != null && !isNaN(state.sensors.lux))
        ? Math.round(Number(state.sensors.lux))
        : (state.sensors.d10 != null && !isNaN(state.sensors.d10))
        ? Math.round(Number(state.sensors.d10))
        : (DOM.sensorLuxVal && !isNaN(parseInt(DOM.sensorLuxVal.textContent.replace(/,/g, ''), 10)) && DOM.sensorLuxVal.textContent !== '---')
        ? parseInt(DOM.sensorLuxVal.textContent.replace(/,/g, ''), 10)
        : null;

      return { temp1: t1, temp2: t2, temp3: t3, lux };
    },

    async sanitizeDummyRecords() {
      if (!this.records || this.records.length === 0) return;

      // กรองเฉพาะข้อมูล mock ดั้งเดิมที่เป็นตัวเลขจำลองตายตัว หรือชุด sample mock เดิมที่ outdoor สูงผิดปกติ (45-60°C)
      const isLegacyMockPlaceholder = (r) => {
        if (!r) return true;
        if (r.temp1 === 25.0 && r.temp2 === 28.5 && r.temp3 === 16.0) return true;
        if (r.temp1 === 31.0 && r.temp2 === 49.0 && r.temp3 === 29.0 && r.lux === 394 && r.source !== 'manual') return true;
        if (r.source === 'sample' && r.temp2 >= 45.0 && r.temp1 <= 33.0) return true;
        return false;
      };

      const hasLegacy = this.records.some(isLegacyMockPlaceholder);
      if (!hasLegacy) return;

      this.records = this.records.filter((r) => !isLegacyMockPlaceholder(r));

      if (this.db) {
        try {
          const tx = this.db.transaction([this.STORE_NAME], 'readwrite');
          const store = tx.objectStore(this.STORE_NAME);
          store.clear();
          this.records.forEach((r) => store.put(r));
        } catch (e) { }
      }
      this.syncFallback();
    },

    async logCurrentSnapshot(source = 'manual') {
      const now = new Date();
      const timestamp = now.getTime();
      const vals = this.getCurrentSensorValues();

      if (vals.temp1 == null && vals.temp2 == null && vals.temp3 == null && vals.lux == null) {
        if (source === 'manual') {
          showToast('warning', 'ยังไม่ได้รับข้อมูลเซนเซอร์จริงจาก ESP32 / PLC กรุณารอสักครู่');
          addLog('warning', '[ประวัติเซนเซอร์] ไม่สามารถบันทึกได้เนื่องจากยังไม่มีข้อมูลเซนเซอร์จริง');
        }
        return;
      }

      const dd = String(now.getDate()).padStart(2, '0');
      const mm = String(now.getMonth() + 1).padStart(2, '0');
      const yyyy = now.getFullYear();
      const dateStr = `${dd}/${mm}/${yyyy}`;

      const hh = String(now.getHours()).padStart(2, '0');
      const mi = String(now.getMinutes()).padStart(2, '0');
      const ss = String(now.getSeconds()).padStart(2, '0');
      const timeStr = `${hh}:${mi}:${ss}`;

      const rec = {
        timestamp,
        iso: now.toISOString(),
        dateStr,
        timeStr,
        temp1: vals.temp1,
        temp2: vals.temp2,
        temp3: vals.temp3,
        lux: vals.lux,
        source,
      };

      await this.saveRecord(rec);
      await this.pruneExpiredRecords();

      this.lastLogTime = timestamp;
      try { localStorage.setItem(this.LAST_LOG_KEY, String(timestamp)); } catch (e) { }

      this.render();
      addLog('info', `[ประวัติเซนเซอร์] บันทึกข้อมูลจริง (${source === 'auto' ? 'อัตโนมัติ 30 นาที' : 'บันทึกทันที'}): S1=${vals.temp1}°C, S2=${vals.temp2}°C, S3=${vals.temp3}°C, Lux=${vals.lux?.toLocaleString()}`);

      if (source === 'manual') {
        showToast('success', 'บันทึกค่าอุณหภูมิและแสงจริงสำเร็จ!');
      }
    },

    checkAndAutoLog(isInit = false) {
      const now = Date.now();
      const vals = this.getCurrentSensorValues();
      if (vals.temp1 == null && vals.temp2 == null && vals.temp3 == null && vals.lux == null) {
        return;
      }
      if (this.lastLogTime === 0) {
        if (isInit) return;
        this.logCurrentSnapshot('auto');
        return;
      }
      const elapsed = now - this.lastLogTime;
      if (elapsed >= this.LOG_INTERVAL_MS) {
        this.logCurrentSnapshot('auto');
      }
    },

    onTelemetry() {
      // Telemetry จริงเข้ามาจาก ESP32 / PLC
      const vals = this.getCurrentSensorValues();
      const now = Date.now();
      if (vals.temp1 != null || vals.temp2 != null || vals.temp3 != null || vals.lux != null) {
        ['temp1', 'temp2', 'temp3', 'lux'].forEach((key) => {
          const v = vals[key];
          if (v != null && !isNaN(v)) {
            if (!this.liveSamples) {
              this.liveSamples = { temp1: [], temp2: [], temp3: [], lux: [], maxSamples: 2000 };
            }
            if (!this.liveSamples[key]) this.liveSamples[key] = [];
            this.liveSamples[key].push({ t: now, v: Number(v) });
            const maxS = this.liveSamples.maxSamples || 2000;
            if (this.liveSamples[key].length > maxS) {
              this.liveSamples[key].shift();
            }
          }
        });
      }

      this.checkAndAutoLog(false);
      this.updateCountdownUI();

      // หน่วงเวลาการคำนวณสถิติหนักๆ ให้ทำไม่เกินทุก 3 วินาที เพื่อไม่ให้เบราว์เซอร์ค้างเวลาข้อมูล MQTT เข้ามาเร็ว
      const nowTs = Date.now();
      if (!this._lastStatsRender || (nowTs - this._lastStatsRender >= 3000)) {
        this._lastStatsRender = nowTs;
        this.renderStats(this.getFilteredRecords());
      }
    },

    broadcastNewRecord(record) {
      if (!state.mqttClient || !state.mqttClient.connected) return;
      try {
        state.mqttClient.publish(CONFIG.topicSync, JSON.stringify({
          type: 'new_sensor_record',
          senderId: state.clientId,
          record: record
        }), { qos: 1 });
      } catch (e) { }
    },

    publishCloudHistory() {
      if (!state.mqttClient || !state.mqttClient.connected) return;
      try {
        const compactRecords = this.records.slice(-500).map(r => ({
          timestamp: r.timestamp,
          iso: r.iso,
          dateStr: r.dateStr,
          timeStr: r.timeStr,
          temp1: r.temp1,
          temp2: r.temp2,
          temp3: r.temp3,
          lux: r.lux,
          source: r.source || 'auto'
        }));
        const payload = JSON.stringify({
          updatedAt: Date.now(),
          senderId: state.clientId,
          records: compactRecords
        });
        state.mqttClient.publish(CONFIG.topicHistorySync, payload, { qos: 1, retain: true });
        this.updateCloudSyncStatus('synced', `ซิงค์คลาวด์แล้ว (${this.records.length} รายการ)`);
      } catch (e) {
        console.warn('Failed to publish cloud history:', e);
      }
    },

    onRemoteRecordReceived(record) {
      if (!record || !record.timestamp) return;
      if (this.records.some(r => r.timestamp === record.timestamp)) return;
      this.records.push(record);
      this.records.sort((a, b) => a.timestamp - b.timestamp);
      this.lastLogTime = Math.max(this.lastLogTime, record.timestamp);
      try { localStorage.setItem(this.LAST_LOG_KEY, String(this.lastLogTime)); } catch (e) {}

      if (this.db) {
        try {
          const tx = this.db.transaction([this.STORE_NAME], 'readwrite');
          const store = tx.objectStore(this.STORE_NAME);
          store.put(record);
        } catch (e) {}
      }
      this.syncFallback();
      this.render();
      this.updateCloudSyncStatus('synced', `รับข้อมูลจากเครื่องอื่น (${this.records.length} รายการ)`);
      addLog('info', `[คลาวด์ซิงค์] ได้รับข้อมูลเซนเซอร์ใหม่จากอุปกรณ์อื่น: S1=${record.temp1}°C, S2=${record.temp2}°C, S3=${record.temp3}°C, Lux=${record.lux}`);
    },

    onRemoteClearReceived() {
      this.records = [];
      this.lastLogTime = 0;
      this.liveSamples = { temp1: [], temp2: [], temp3: [], lux: [], maxSamples: 2000 };
      try {
        localStorage.setItem(this.LAST_LOG_KEY, '0');
        localStorage.removeItem(this.STORAGE_KEY);
      } catch (e) {}
      if (this.db) {
        try {
          const tx = this.db.transaction([this.STORE_NAME], 'readwrite');
          const store = tx.objectStore(this.STORE_NAME);
          store.clear();
        } catch (e) {}
      }
      this.render();
      this.updateCloudSyncStatus('synced', 'ล้างข้อมูลตรงกันทุกเครื่อง');
      addLog('info', '[คลาวด์ซิงค์] อุปกรณ์อื่นทำการล้างประวัติข้อมูลเซนเซอร์');
    },

    mergeRemoteRecords(incomingList) {
      if (!Array.isArray(incomingList) || incomingList.length === 0) return;
      const existingTimestamps = new Set(this.records.map(r => r.timestamp));
      let added = 0;
      for (const r of incomingList) {
        if (r && r.timestamp && !existingTimestamps.has(r.timestamp)) {
          this.records.push(r);
          existingTimestamps.add(r.timestamp);
          added++;
        }
      }
      if (added > 0) {
        this.records.sort((a, b) => a.timestamp - b.timestamp);
        if (this.records.length > 0) {
          this.lastLogTime = Math.max(this.lastLogTime, this.records[this.records.length - 1].timestamp);
          try { localStorage.setItem(this.LAST_LOG_KEY, String(this.lastLogTime)); } catch (e) {}
        }
        this.syncFallback();
        if (this.db) {
          try {
            const tx = this.db.transaction([this.STORE_NAME], 'readwrite');
            const store = tx.objectStore(this.STORE_NAME);
            this.records.forEach(rec => store.put(rec));
          } catch (e) {}
        }
        this.render();
        this.updateCloudSyncStatus('synced', `ซิงค์สำเร็จ (+${added} รวม ${this.records.length} รายการ)`);
        addLog('info', `[คลาวด์ซิงค์] ซิงค์ประวัติเซนเซอร์ข้ามเครื่องสำเร็จ (+${added} รายการ รวม ${this.records.length} รายการ)`);
      } else {
        this.updateCloudSyncStatus('synced', `ข้อมูลตรงกันแล้ว (${this.records.length} รายการ)`);
      }
    },

    updateCloudSyncStatus(status = 'synced', msg = '') {
      const badge = DOM.historyCloudSyncBadge || document.getElementById('historyCloudSyncBadge');
      const textEl = DOM.historyCloudSyncText || document.getElementById('historyCloudSyncText');
      if (!textEl) return;
      textEl.textContent = msg || (status === 'synced' ? 'ซิงค์เรียบร้อย' : 'กำลังซิงค์...');
      if (badge) {
        badge.classList.toggle('is-syncing', status === 'syncing');
      }
    },

    startTicker() {
      if (this.tickerTimer) clearInterval(this.tickerTimer);
      this.tickerTimer = setInterval(() => {
        this.checkAndAutoLog(false);
        this.updateCountdownUI();
      }, 1000);
    },

    updateCountdownUI() {
      const now = Date.now();
      const countdownEl = document.getElementById('historyCountdownText');
      const totalEl = document.getElementById('historyTotalCount');
      const lastSavedEl = document.getElementById('historyLastSavedText');

      if (totalEl) {
        totalEl.textContent = this.records.length.toLocaleString();
      }

      if (lastSavedEl) {
        if (this.records.length > 0) {
          const latest = this.records[this.records.length - 1];
          lastSavedEl.textContent = `${latest.dateStr} ${latest.timeStr}`;
        } else {
          lastSavedEl.textContent = 'ยังไม่มีการบันทึก';
        }
      }

      if (countdownEl) {
        if (this.lastLogTime === 0) {
          countdownEl.textContent = 'พร้อมบันทึก';
          return;
        }
        const nextTime = this.lastLogTime + this.LOG_INTERVAL_MS;
        const diff = Math.max(0, nextTime - now);
        const mins = Math.floor(diff / 60000);
        const secs = Math.floor((diff % 60000) / 1000);
        countdownEl.textContent = `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
      }
    },

    isTimestampInCurrentRange(ts) {
      if (!ts || isNaN(ts)) return false;
      const now = Date.now();
      if (this.currentRange === 'today') {
        const startOfToday = new Date();
        startOfToday.setHours(0, 0, 0, 0);
        return ts >= startOfToday.getTime();
      } else if (this.currentRange === '24h') {
        return ts >= now - 24 * 3600 * 1000;
      } else if (this.currentRange === '7d') {
        return ts >= now - 7 * 86400 * 1000;
      } else if (this.currentRange === '30d') {
        return ts >= now - 30 * 86400 * 1000;
      } else if (this.currentRange === 'custom') {
        let valid = true;
        if (this.customStart) {
          const s = new Date(this.customStart + 'T00:00:00').getTime();
          valid = valid && (ts >= s);
        }
        if (this.customEnd) {
          const e = new Date(this.customEnd + 'T23:59:59').getTime();
          valid = valid && (ts <= e);
        }
        return valid;
      }
      return true;
    },

    getFilteredRecords() {
      return this.records.filter((r) => this.isTimestampInCurrentRange(r.timestamp));
    },

    render() {
      const filtered = this.getFilteredRecords();
      this.updateCountdownUI();
      this.renderStats(filtered);
      this.renderChart(filtered);
      this.renderTable(filtered);
    },

    renderStats(filtered) {
      const live = this.getCurrentSensorValues();
      const calcMetrics = (arr, key, liveVal) => {
        // 1. ค่าจากประวัติสแนปช็อตย้อนหลัง (Snapshot Records)
        const histVals = arr
          .map((r) => r[key])
          .filter((v) => v != null && !isNaN(v))
          .map(Number);

        // 2. ค่าตัวอย่างแบบ Live Telemetry สตรีมมิ่งในเซสชันปัจจุบันที่อยู่ในช่วงเวลาที่เลือก
        const liveList = (this.liveSamples && this.liveSamples[key]) ? this.liveSamples[key] : [];
        const activeLiveVals = liveList
          .filter((s) => this.isTimestampInCurrentRange(s.t))
          .map((s) => s.v);

        // หากมี liveVal ปัจจุบันจาก MQTT
        if (liveVal != null && !isNaN(liveVal)) {
          if (activeLiveVals.length === 0) {
            activeLiveVals.push(Number(liveVal));
          }
        }

        // ค่าปัจจุบันที่จะแสดงผล
        const cur = (liveVal != null && !isNaN(liveVal))
          ? Number(liveVal)
          : (activeLiveVals.length > 0
              ? activeLiveVals[activeLiveVals.length - 1]
              : (histVals.length > 0 ? histVals[histVals.length - 1] : '--'));

        // รวมค่าทั้งหมดเพื่อหา Min และ Max
        const allPool = [...histVals, ...activeLiveVals];
        if (liveVal != null && !isNaN(liveVal) && !allPool.includes(Number(liveVal))) {
          allPool.push(Number(liveVal));
        }

        if (allPool.length === 0) {
          return { cur, min: '--', max: '--', avg: '--' };
        }

        const min = Math.min(...allPool);
        const max = Math.max(...allPool);

        // 3. คำนวณค่าเฉลี่ยแบบ Dynamic & Reactive:
        // นำทั้งประวัติสแนปช็อต (30 นาที) และข้อมูล Live Telemetry สตรีมมิ่งสดมาร่วมเฉลี่ยอย่างสมดุล
        let avg;
        if (histVals.length > 0) {
          const liveSum = activeLiveVals.reduce((a, b) => a + b, 0);
          const currentSessionAvg = activeLiveVals.length > 0 ? (liveSum / activeLiveVals.length) : null;
          const histSum = histVals.reduce((a, b) => a + b, 0);
          if (currentSessionAvg != null) {
            avg = (histSum + currentSessionAvg) / (histVals.length + 1);
          } else {
            avg = histSum / histVals.length;
          }
        } else {
          const sum = activeLiveVals.reduce((a, b) => a + b, 0);
          avg = activeLiveVals.length > 0 ? (sum / activeLiveVals.length) : (liveVal != null ? Number(liveVal) : '--');
        }

        return { cur, min, max, avg };
      };

      const mS1 = calcMetrics(filtered, 'temp1', live.temp1);
      const mS2 = calcMetrics(filtered, 'temp2', live.temp2);
      const mS3 = calcMetrics(filtered, 'temp3', live.temp3);
      const mLux = calcMetrics(filtered, 'lux', live.lux);

      // S1
      const curS1 = document.getElementById('statCurS1');
      const minS1 = document.getElementById('statMinS1');
      const maxS1 = document.getElementById('statMaxS1');
      const avgS1 = document.getElementById('statAvgS1');
      if (curS1) curS1.textContent = typeof mS1.cur === 'number' ? mS1.cur.toFixed(1) : mS1.cur;
      if (minS1) minS1.textContent = typeof mS1.min === 'number' ? `${mS1.min.toFixed(1)} °C` : '--.- °C';
      if (maxS1) maxS1.textContent = typeof mS1.max === 'number' ? `${mS1.max.toFixed(1)} °C` : '--.- °C';
      if (avgS1) {
        avgS1.textContent = typeof mS1.avg === 'number' ? `${mS1.avg.toFixed(1)} °C` : '--.- °C';
        if (typeof mS1.avg === 'number') avgS1.title = `ค่าเฉลี่ยเซนเซอร์ 1: ${mS1.avg.toFixed(2)} °C (อัปเดตสดตามฮาร์ดแวร์)`;
      }

      // S2
      const curS2 = document.getElementById('statCurS2');
      const minS2 = document.getElementById('statMinS2');
      const maxS2 = document.getElementById('statMaxS2');
      const avgS2 = document.getElementById('statAvgS2');
      if (curS2) curS2.textContent = typeof mS2.cur === 'number' ? mS2.cur.toFixed(1) : mS2.cur;
      if (minS2) minS2.textContent = typeof mS2.min === 'number' ? `${mS2.min.toFixed(1)} °C` : '--.- °C';
      if (maxS2) maxS2.textContent = typeof mS2.max === 'number' ? `${mS2.max.toFixed(1)} °C` : '--.- °C';
      if (avgS2) {
        avgS2.textContent = typeof mS2.avg === 'number' ? `${mS2.avg.toFixed(1)} °C` : '--.- °C';
        if (typeof mS2.avg === 'number') avgS2.title = `ค่าเฉลี่ยเซนเซอร์ 2: ${mS2.avg.toFixed(2)} °C (อัปเดตสดตามฮาร์ดแวร์)`;
      }

      // S3
      const curS3 = document.getElementById('statCurS3');
      const minS3 = document.getElementById('statMinS3');
      const maxS3 = document.getElementById('statMaxS3');
      const avgS3 = document.getElementById('statAvgS3');
      if (curS3) curS3.textContent = typeof mS3.cur === 'number' ? mS3.cur.toFixed(1) : mS3.cur;
      if (minS3) minS3.textContent = typeof mS3.min === 'number' ? `${mS3.min.toFixed(1)} °C` : '--.- °C';
      if (maxS3) maxS3.textContent = typeof mS3.max === 'number' ? `${mS3.max.toFixed(1)} °C` : '--.- °C';
      if (avgS3) {
        avgS3.textContent = typeof mS3.avg === 'number' ? `${mS3.avg.toFixed(1)} °C` : '--.- °C';
        if (typeof mS3.avg === 'number') avgS3.title = `ค่าเฉลี่ยเซนเซอร์ 3: ${mS3.avg.toFixed(2)} °C (อัปเดตสดตามฮาร์ดแวร์)`;
      }

      // Lux
      const curLux = document.getElementById('statCurLux');
      const minLux = document.getElementById('statMinLux');
      const maxLux = document.getElementById('statMaxLux');
      const avgLux = document.getElementById('statAvgLux');
      if (curLux) curLux.textContent = typeof mLux.cur === 'number' ? mLux.cur.toLocaleString() : mLux.cur;
      if (minLux) minLux.textContent = typeof mLux.min === 'number' ? `${mLux.min.toLocaleString()} Lux` : '-- Lux';
      if (maxLux) maxLux.textContent = typeof mLux.max === 'number' ? `${mLux.max.toLocaleString()} Lux` : '-- Lux';
      if (avgLux) {
        avgLux.textContent = typeof mLux.avg === 'number' ? `${Math.round(mLux.avg).toLocaleString()} Lux` : '-- Lux';
        if (typeof mLux.avg === 'number') avgLux.title = `ค่าเฉลี่ยความเข้มแสง: ${mLux.avg.toFixed(1)} Lux (อัปเดตสดตามฮาร์ดแวร์)`;
      }
    },

    renderChart(filtered) {
      if (!this.canvas) {
        this.canvas = document.getElementById('historyCanvas');
        if (this.canvas) this.ctx = this.canvas.getContext('2d');
      }
      if (!this.canvas || !this.ctx) return;

      const emptyEl = document.getElementById('historyChartEmpty');
      const subtitleEl = document.getElementById('historyChartRangeSubtitle');

      if (subtitleEl) {
        const map = {
          today: 'กำลังแสดงข้อมูลวันนี้',
          '24h': 'กำลังแสดงข้อมูล 24 ชั่วโมงล่าสุด',
          '7d': 'กำลังแสดงข้อมูล 7 วันล่าสุด',
          '30d': 'กำลังแสดงข้อมูล 30 วันทั้งหมด',
          custom: `กำหนดเอง (${this.customStart || '...'} ถึง ${this.customEnd || '...'})`,
        };
        subtitleEl.textContent = `${map[this.currentRange] || ''} (พบ ${filtered.length} จุดข้อมูล)`;
      }

      if (filtered.length === 0) {
        if (emptyEl) emptyEl.style.display = 'flex';
        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        return;
      } else {
        if (emptyEl) emptyEl.style.display = 'none';
      }

      // Handle HiDPI scaling safely without destroying GPU buffer on every redraw
      const dpr = window.devicePixelRatio || 1;
      const rect = this.canvas.getBoundingClientRect();
      const width = rect.width;
      const height = rect.height;
      const targetW = Math.round(width * dpr);
      const targetH = Math.round(height * dpr);
      if (this.canvas.width !== targetW || this.canvas.height !== targetH) {
        this.canvas.width = targetW;
        this.canvas.height = targetH;
        if (this.ctx.resetTransform) {
          this.ctx.resetTransform();
        } else {
          this.ctx.setTransform(1, 0, 0, 1, 0, 0);
        }
        this.ctx.scale(dpr, dpr);
      } else {
        this.ctx.clearRect(0, 0, width, height);
      }

      const padding = { top: 25, right: 65, bottom: 40, left: 55 };
      const plotW = width - padding.left - padding.right;
      const plotH = height - padding.top - padding.bottom;

      if (plotW <= 0 || plotH <= 0) return;

      // Min/Max for Temp
      let allTemps = [];
      if (this.chartMode !== 'lux') {
        filtered.forEach((r) => {
          if (this.activeSensors.s1 && r.temp1 != null) allTemps.push(r.temp1);
          if (this.activeSensors.s2 && r.temp2 != null) allTemps.push(r.temp2);
          if (this.activeSensors.s3 && r.temp3 != null) allTemps.push(r.temp3);
        });
      }
      let minTemp = allTemps.length > 0 ? Math.floor(Math.min(...allTemps) - 2) : 15;
      let maxTemp = allTemps.length > 0 ? Math.ceil(Math.max(...allTemps) + 2) : 40;
      if (minTemp >= maxTemp) { minTemp = 15; maxTemp = 40; }

      // Min/Max for Lux
      let allLux = [];
      if (this.chartMode !== 'temp' && this.activeSensors.lux) {
        filtered.forEach((r) => {
          if (r.lux != null) allLux.push(r.lux);
        });
      }
      let maxLux = allLux.length > 0 ? Math.max(...allLux) : 1000;
      maxLux = Math.ceil(Math.max(500, maxLux * 1.1) / 100) * 100;
      const minLux = 0;

      // Draw Grid & Y-Axis Labels
      this.ctx.strokeStyle = '#e2e8f0';
      this.ctx.lineWidth = 1;
      this.ctx.font = '11px Prompt, Kanit, sans-serif';
      this.ctx.fillStyle = '#64748b';

      const gridSteps = 5;
      for (let i = 0; i <= gridSteps; i++) {
        const y = padding.top + (plotH * (gridSteps - i)) / gridSteps;
        this.ctx.beginPath();
        this.ctx.moveTo(padding.left, y);
        this.ctx.lineTo(padding.left + plotW, y);
        this.ctx.stroke();

        // Left Temp Axis
        if (this.chartMode !== 'lux') {
          const tVal = minTemp + ((maxTemp - minTemp) * i) / gridSteps;
          this.ctx.textAlign = 'right';
          this.ctx.textBaseline = 'middle';
          this.ctx.fillStyle = '#0284c7';
          this.ctx.fillText(`${tVal.toFixed(0)}°C`, padding.left - 8, y);
        }

        // Right Lux Axis
        if (this.chartMode !== 'temp' && this.activeSensors.lux) {
          const lVal = minLux + ((maxLux - minLux) * i) / gridSteps;
          this.ctx.textAlign = 'left';
          this.ctx.textBaseline = 'middle';
          this.ctx.fillStyle = '#b45309';
          this.ctx.fillText(`${Math.round(lVal)}lx`, padding.left + plotW + 8, y);
        }
      }

      // X-Axis Scale & Labels
      const minTime = filtered[0].timestamp;
      const maxTime = filtered[filtered.length - 1].timestamp;
      const timeSpan = maxTime - minTime || 1;

      const getX = (t) => padding.left + ((t - minTime) / timeSpan) * plotW;
      const getYTemp = (temp) => padding.top + plotH - ((temp - minTemp) / (maxTemp - minTemp)) * plotH;
      const getYLux = (lux) => padding.top + plotH - ((lux - minLux) / (maxLux - minLux)) * plotH;

      // Draw X Grid Labels
      const xLabelCount = Math.min(6, filtered.length);
      this.ctx.fillStyle = '#64748b';
      this.ctx.textAlign = 'center';
      this.ctx.textBaseline = 'top';

      for (let i = 0; i < xLabelCount; i++) {
        const idx = Math.floor((i * (filtered.length - 1)) / (xLabelCount - 1 || 1));
        const r = filtered[idx];
        const x = getX(r.timestamp);

        this.ctx.beginPath();
        this.ctx.moveTo(x, padding.top + plotH);
        this.ctx.lineTo(x, padding.top + plotH + 4);
        this.ctx.stroke();

        const label = (this.currentRange === 'today' || this.currentRange === '24h') ? r.timeStr.substring(0, 5) : `${r.dateStr.substring(0, 5)} ${r.timeStr.substring(0, 5)}`;
        this.ctx.fillText(label, x, padding.top + plotH + 8);
      }

      // Draw Line Series
      const drawLine = (key, color, isLux = false) => {
        this.ctx.beginPath();
        this.ctx.strokeStyle = color;
        this.ctx.lineWidth = isLux ? 2 : 2.5;
        this.ctx.lineJoin = 'round';
        this.ctx.lineCap = 'round';

        let started = false;
        filtered.forEach((r) => {
          const val = r[key];
          if (val == null || isNaN(val)) return;
          const x = getX(r.timestamp);
          const y = isLux ? getYLux(val) : getYTemp(val);
          if (!started) {
            this.ctx.moveTo(x, y);
            started = true;
          } else {
            this.ctx.lineTo(x, y);
          }
        });
        this.ctx.stroke();

        if (filtered.length <= 40) {
          filtered.forEach((r) => {
            const val = r[key];
            if (val == null || isNaN(val)) return;
            const x = getX(r.timestamp);
            const y = isLux ? getYLux(val) : getYTemp(val);
            this.ctx.beginPath();
            this.ctx.arc(x, y, 3, 0, Math.PI * 2);
            this.ctx.fillStyle = '#ffffff';
            this.ctx.fill();
            this.ctx.strokeStyle = color;
            this.ctx.lineWidth = 2;
            this.ctx.stroke();
          });
        }
      };

      if (this.chartMode !== 'temp' && this.activeSensors.lux) {
        drawLine('lux', '#eab308', true);
      }
      if (this.chartMode !== 'lux') {
        if (this.activeSensors.s3) drawLine('temp3', '#8b5cf6', false);
        if (this.activeSensors.s2) drawLine('temp2', '#f97316', false);
        if (this.activeSensors.s1) drawLine('temp1', '#0284c7', false);
      }

      // Hover Crosshair & Highlight
      if (this.hoverIndex >= 0 && this.hoverIndex < filtered.length) {
        const hoverRec = filtered[this.hoverIndex];
        const hX = getX(hoverRec.timestamp);

        this.ctx.beginPath();
        this.ctx.setLineDash([4, 4]);
        this.ctx.strokeStyle = '#94a3b8';
        this.ctx.lineWidth = 1.5;
        this.ctx.moveTo(hX, padding.top);
        this.ctx.lineTo(hX, padding.top + plotH);
        this.ctx.stroke();
        this.ctx.setLineDash([]);

        const highlightDot = (val, color, isLux = false) => {
          if (val == null || isNaN(val)) return;
          const hY = isLux ? getYLux(val) : getYTemp(val);
          this.ctx.beginPath();
          this.ctx.arc(hX, hY, 6, 0, Math.PI * 2);
          this.ctx.fillStyle = color;
          this.ctx.fill();
          this.ctx.lineWidth = 2;
          this.ctx.strokeStyle = '#ffffff';
          this.ctx.stroke();
        };

        if (this.chartMode !== 'temp' && this.activeSensors.lux) highlightDot(hoverRec.lux, '#eab308', true);
        if (this.chartMode !== 'lux') {
          if (this.activeSensors.s3) highlightDot(hoverRec.temp3, '#8b5cf6', false);
          if (this.activeSensors.s2) highlightDot(hoverRec.temp2, '#f97316', false);
          if (this.activeSensors.s1) highlightDot(hoverRec.temp1, '#0284c7', false);
        }
      }
    },

    renderTable(filtered) {
      const tbody = document.getElementById('historyTableBody');
      const countEl = document.getElementById('historyTableCountText');
      const infoEl = document.getElementById('historyPaginationInfo');
      const curPageEl = document.getElementById('historyPageCurrentText');
      const prevBtn = document.getElementById('historyPrevPageBtn');
      const nextBtn = document.getElementById('historyNextPageBtn');

      if (!tbody) return;

      let tableData = [...filtered];
      if (this.searchQuery.trim()) {
        const q = this.searchQuery.trim().toLowerCase();
        tableData = tableData.filter((r) => {
          return (
            r.dateStr.toLowerCase().includes(q) ||
            r.timeStr.toLowerCase().includes(q) ||
            String(r.temp1).includes(q) ||
            String(r.temp2).includes(q) ||
            String(r.temp3).includes(q) ||
            String(r.lux).includes(q) ||
            (r.source && r.source.toLowerCase().includes(q))
          );
        });
      }

      tableData.reverse();

      const totalItems = tableData.length;
      if (countEl) countEl.textContent = `แสดง ${totalItems.toLocaleString()} รายการ`;

      if (totalItems === 0) {
        tbody.innerHTML = `
          <tr>
            <td colspan="8" class="history-table-empty">
              📭 ไม่พบข้อมูลประวัติเซนเซอร์ที่ตรงกับเงื่อนไข
            </td>
          </tr>
        `;
        if (infoEl) infoEl.textContent = 'แสดง 0 - 0 จาก 0 รายการ';
        if (curPageEl) curPageEl.textContent = 'หน้า 1 / 1';
        if (prevBtn) prevBtn.disabled = true;
        if (nextBtn) nextBtn.disabled = true;
        return;
      }

      const totalPages = Math.ceil(totalItems / this.pageSize) || 1;
      if (this.currentPage > totalPages) this.currentPage = totalPages;
      if (this.currentPage < 1) this.currentPage = 1;

      const startIdx = (this.currentPage - 1) * this.pageSize;
      const endIdx = Math.min(startIdx + this.pageSize, totalItems);
      const pageRows = tableData.slice(startIdx, endIdx);

      if (infoEl) {
        infoEl.textContent = `แสดง ${(startIdx + 1).toLocaleString()} - ${endIdx.toLocaleString()} จาก ${totalItems.toLocaleString()} รายการ`;
      }
      if (curPageEl) {
        curPageEl.textContent = `หน้า ${this.currentPage} / ${totalPages}`;
      }
      if (prevBtn) prevBtn.disabled = this.currentPage <= 1;
      if (nextBtn) nextBtn.disabled = this.currentPage >= totalPages;

      let html = '';
      pageRows.forEach((r, idx) => {
        const rowNum = totalItems - (startIdx + idx);
        const s1 = r.temp1 != null ? `${Number(r.temp1).toFixed(1)} °C` : '--';
        const s2 = r.temp2 != null ? `${Number(r.temp2).toFixed(1)} °C` : '--';
        const s3 = r.temp3 != null ? `${Number(r.temp3).toFixed(1)} °C` : '--';
        const lux = r.lux != null ? `${Number(r.lux).toLocaleString()} Lux` : '--';
        const isAuto = r.source === 'auto';
        const isSample = r.source === 'sample';
        const badgeClass = isSample ? 'history-badge-source--sample' : (isAuto ? 'history-badge-source--auto' : 'history-badge-source--manual');
        const badgeText = isSample ? 'ข้อมูลตัวอย่าง' : (isAuto ? 'อัตโนมัติ 30 นาที' : 'บันทึกทันที');

        html += `
          <tr>
            <td style="color:var(--text-muted);">${rowNum}</td>
            <td><strong>${escapeHtml(r.dateStr)}</strong></td>
            <td>${escapeHtml(r.timeStr)}</td>
            <td class="td--s1">${s1}</td>
            <td class="td--s2">${s2}</td>
            <td class="td--s3">${s3}</td>
            <td class="td--lux">${lux}</td>
            <td><span class="history-badge-source ${badgeClass}">${badgeText}</span></td>
          </tr>
        `;
      });

      tbody.innerHTML = html;
    },

    exportExcel() {
      const filtered = this.getFilteredRecords();
      if (filtered.length === 0) {
        showToast('ไม่มีข้อมูลสำหรับส่งออก', 'warning');
        return;
      }

      // Check if XLSX library is available
      if (typeof XLSX === 'undefined') {
        console.warn('[SensorHistory] XLSX library not loaded, falling back to CSV export');
        this.exportCSV();
        return;
      }

      try {
        const wb = XLSX.utils.book_new();

        // ─────────────────────────────────────────────
        // 1. SHEET 1: ข้อมูลเซนเซอร์ (Raw Data)
        // ─────────────────────────────────────────────
        const rawSheetData = [];
        
        rawSheetData.push(['มหาวิทยาลัยเทคโนโลยีราชมงคลอีสาน วิทยาเขตสุรินทร์ — ระบบควบคุมเครื่องปรับอากาศ']);
        rawSheetData.push(['รายงานบันทึกประวัติค่าอุณหภูมิและความเข้มแสง (บันทึกทุก 30 นาที ย้อนหลัง 30 วัน)']);
        rawSheetData.push([
          `วันที่ส่งออกข้อมูล: ${new Date().toLocaleDateString('th-TH')} ${new Date().toLocaleTimeString('th-TH')}`,
          '',
          '',
          `จำนวนรายการทั้งหมด: ${filtered.length} รายการ`,
          '',
          `ช่วงเวลา: ${this.currentRange}`
        ]);
        rawSheetData.push([]); // blank row

        // Column Headers
        rawSheetData.push([
          'ลำดับ',
          'วันที่',
          'เวลา',
          'วันเวลา ISO',
          'เซนเซอร์ 1: Indoor (°C)',
          'เซนเซอร์ 2: Outdoor (°C)',
          'เซนเซอร์ 3: อินเวอร์เตอร์ (°C)',
          'ความเข้มแสง: Ambient (Lux)',
          'ประเภทการบันทึก'
        ]);

        filtered.forEach((r, idx) => {
          rawSheetData.push([
            idx + 1,
            r.dateStr,
            r.timeStr,
            r.iso,
            r.temp1 != null ? Number(r.temp1) : '',
            r.temp2 != null ? Number(r.temp2) : '',
            r.temp3 != null ? Number(r.temp3) : '',
            r.lux != null ? Number(r.lux) : '',
            r.source === 'auto' ? 'อัตโนมัติ (30 นาที)' : 'บันทึกทันที'
          ]);
        });

        const wsRaw = XLSX.utils.aoa_to_sheet(rawSheetData);
        wsRaw['!cols'] = [
          { wch: 8 },  // ลำดับ
          { wch: 14 }, // วันที่
          { wch: 12 }, // เวลา
          { wch: 26 }, // ISO
          { wch: 24 }, // S1
          { wch: 24 }, // S2
          { wch: 26 }, // S3
          { wch: 26 }, // Lux
          { wch: 20 }, // ประเภท
        ];
        XLSX.utils.book_append_sheet(wb, wsRaw, 'ข้อมูลเซนเซอร์');

        // ─────────────────────────────────────────────
        // 2. SHEET 2: สรุปสถิติเพื่อวิเคราะห์ (Analytics & Statistics)
        // ─────────────────────────────────────────────
        const calcStats = (key) => {
          const vals = filtered.map(r => r[key]).filter(v => v != null && !isNaN(v));
          if (vals.length === 0) return { count: 0, min: 0, max: 0, avg: 0, sd: 0, range: 0, latest: 0 };
          const count = vals.length;
          const min = Math.min(...vals);
          const max = Math.max(...vals);
          const sum = vals.reduce((a, b) => a + b, 0);
          const avg = sum / count;
          const variance = vals.reduce((a, b) => a + Math.pow(b - avg, 2), 0) / count;
          const sd = Math.sqrt(variance);
          const range = max - min;
          const latest = vals[vals.length - 1];
          return { count, min, max, avg, sd, range, latest };
        };

        const s1Stats = calcStats('temp1');
        const s2Stats = calcStats('temp2');
        const s3Stats = calcStats('temp3');
        const luxStats = calcStats('lux');

        const analyticsData = [];
        analyticsData.push(['ตารางสรุปสถิติเชิงวิเคราะห์ (Statistical Summary for Data Analytics)']);
        analyticsData.push([`สร้างเมื่อ: ${new Date().toLocaleString('th-TH')} | แหล่งข้อมูล: HiveMQ MQTT & AMX FX3U PLC`]);
        analyticsData.push([]);
        analyticsData.push([
          'รายการเซนเซอร์',
          'ตำแหน่งติดตั้ง / หน้าที่',
          'หน่วยวัด',
          'ค่าล่าสุด',
          'ค่าเฉลี่ย (Mean/Avg)',
          'ค่าต่ำสุด (Min)',
          'ค่าสูงสุด (Max)',
          'ผลต่าง (Max - Min)',
          'ส่วนเบี่ยงเบนมาตรฐาน (SD)',
          'จำนวนจุดข้อมูล (N)'
        ]);

        analyticsData.push([
          'เซนเซอร์ 1',
          'Indoor (ภายในห้องปรับอากาศ)',
          '°C',
          s1Stats.latest,
          parseFloat(s1Stats.avg.toFixed(2)),
          s1Stats.min,
          s1Stats.max,
          parseFloat(s1Stats.range.toFixed(2)),
          parseFloat(s1Stats.sd.toFixed(2)),
          s1Stats.count
        ]);

        analyticsData.push([
          'เซนเซอร์ 2',
          'Outdoor (อุณหภูมิแวดล้อมภายนอก)',
          '°C',
          s2Stats.latest,
          parseFloat(s2Stats.avg.toFixed(2)),
          s2Stats.min,
          s2Stats.max,
          parseFloat(s2Stats.range.toFixed(2)),
          parseFloat(s2Stats.sd.toFixed(2)),
          s2Stats.count
        ]);

        analyticsData.push([
          'เซนเซอร์ 3',
          'Inverter (คอยล์ / ลมจ่ายอินเวอร์เตอร์)',
          '°C',
          s3Stats.latest,
          parseFloat(s3Stats.avg.toFixed(2)),
          s3Stats.min,
          s3Stats.max,
          parseFloat(s3Stats.range.toFixed(2)),
          parseFloat(s3Stats.sd.toFixed(2)),
          s3Stats.count
        ]);

        analyticsData.push([
          'ความเข้มแสง',
          'Ambient Light Sensor (ความสว่างแสงโดยรอบ)',
          'Lux',
          luxStats.latest,
          Math.round(luxStats.avg),
          luxStats.min,
          luxStats.max,
          luxStats.range,
          Math.round(luxStats.sd),
          luxStats.count
        ]);

        analyticsData.push([]);
        analyticsData.push(['คำแนะนำสำหรับการนำไปวิเคราะห์ใน Excel:']);
        analyticsData.push(['1. สามารถใช้สูตร =CORREL(E6:E' + (filtered.length + 5) + ', H6:H' + (filtered.length + 5) + ') ในชีตแรก เพื่อหาความสัมพันธ์ระหว่างอุณหภูมิภายนอกกับความเข้มแสง']);
        analyticsData.push(['2. สามารถใช้ Insert > Recommended Charts เพื่อสร้างกราฟ Scatter Plot หรือ Line Chart เปรียบเทียบประสิทธิภาพ']);
        analyticsData.push(['3. ข้อมูลอุณหภูมิและความเข้มแสงจัดเก็บเป็นตัวเลขจริง (Numeric) สามารถทำ Pivot Table ได้ทันที']);

        const wsAnalytics = XLSX.utils.aoa_to_sheet(analyticsData);
        wsAnalytics['!cols'] = [
          { wch: 18 },
          { wch: 38 },
          { wch: 10 },
          { wch: 12 },
          { wch: 18 },
          { wch: 14 },
          { wch: 14 },
          { wch: 18 },
          { wch: 24 },
          { wch: 20 },
        ];
        XLSX.utils.book_append_sheet(wb, wsAnalytics, 'สรุปสถิติวิเคราะห์');

        // File download
        const now = new Date();
        const datePart = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}_${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(2, '0')}`;
        const fileName = `sensor_analytics_30days_${datePart}.xlsx`;

        XLSX.writeFile(wb, fileName);
        showToast(`ส่งออกไฟล์ Excel (.xlsx) สำเร็จ (${filtered.length} รายการ 2 ชีต)`, 'success');
        addLog('info', `[ประวัติเซนเซอร์] ส่งออกไฟล์ Excel (.xlsx) สำเร็จ: ${fileName}`);
      } catch (err) {
        console.error('[SensorHistory] Error generating Excel file', err);
        showToast('เกิดข้อผิดพลาดในการสร้าง Excel กำลังดาวน์โหลดเป็น CSV แทน', 'warning');
        this.exportCSV();
      }
    },

    exportCSV() {
      const filtered = this.getFilteredRecords();
      if (filtered.length === 0) {
        showToast('ไม่มีข้อมูลสำหรับส่งออก', 'warning');
        return;
      }

      let csv = '\uFEFF';
      csv += 'ลำดับ,วันที่,เวลา,เซนเซอร์ 1 - Indoor (°C),เซนเซอร์ 2 - Outdoor (°C),เซนเซอร์ 3 - Inverter (°C),ความเข้มแสง (Lux),ประเภทการบันทึก,ISO Timestamp\n';

      filtered.forEach((r, idx) => {
        const s1 = r.temp1 != null ? r.temp1 : '';
        const s2 = r.temp2 != null ? r.temp2 : '';
        const s3 = r.temp3 != null ? r.temp3 : '';
        const lux = r.lux != null ? r.lux : '';
        const src = r.source === 'auto' ? 'อัตโนมัติ (30 นาที)' : 'บันทึกทันที';
        csv += `${idx + 1},"${r.dateStr}","${r.timeStr}",${s1},${s2},${s3},${lux},"${src}","${r.iso}"\n`;
      });

      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      const now = new Date();
      const datePart = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}`;
      a.href = url;
      a.download = `sensor_history_30days_${datePart}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);

      showToast('success', `ส่งออกข้อมูล CSV เรียบร้อยแล้ว (${filtered.length} รายการ)`);
      addLog('info', `[ประวัติเซนเซอร์] ส่งออกไฟล์ CSV สำเร็จ: ${filtered.length} รายการ`);
    },

    async generateSample30DayData(silent = false) {
      if (!silent) {
        if (!confirm('ต้องการสร้างข้อมูลตัวอย่างย้อนหลัง 30 วัน (บันทึกทุก 30 นาที รวม 1,440 รายการ) ที่อิงฐานค่าจริงของเซนเซอร์เพื่อวิเคราะห์ใช่หรือไม่?')) {
          return;
        }
      }

      const now = Date.now();
      const interval = this.LOG_INTERVAL_MS;
      const totalPoints = 30 * 48; // 1,440 รายการ (30 วัน)
      const sampleList = [];
      const currentLive = this.getCurrentSensorValues();
      const baseIndoor = (currentLive.temp1 != null && !isNaN(currentLive.temp1)) ? currentLive.temp1 : 28.5;
      const baseOutdoor = (currentLive.temp2 != null && !isNaN(currentLive.temp2)) ? currentLive.temp2 : 31.0;
      const baseInverter = (currentLive.temp3 != null && !isNaN(currentLive.temp3)) ? currentLive.temp3 : 26.5;
      const baseLux = (currentLive.lux != null && !isNaN(currentLive.lux)) ? currentLive.lux : 1500;

      for (let i = totalPoints - 1; i >= 0; i--) {
        const t = new Date(now - i * interval);
        const hour = t.getHours() + t.getMinutes() / 60;

        // วงรอบอุณหภูมิตามแสงอาทิตย์ อิงฐานค่าจริงจากเซนเซอร์ Real-time
        const sunFactor = Math.max(0, Math.sin(((hour - 6) / 12) * Math.PI));
        let outdoorTemp = parseFloat((baseOutdoor - 2.0 + sunFactor * 4.0 + (Math.random() - 0.5) * 0.8).toFixed(1));
        let indoorTemp = parseFloat((baseIndoor - 0.5 + (sunFactor > 0 ? 0.8 : 0.2) + (Math.random() - 0.5) * 0.4).toFixed(1));
        let inverterTemp = parseFloat((baseInverter - 0.5 + sunFactor * 1.0 + (Math.random() - 0.5) * 0.4).toFixed(1));
        let lux = Math.round(sunFactor > 0 ? (baseLux * 0.4 + sunFactor * baseLux * 0.8 + (Math.random() - 0.5) * 50) : (15 + Math.random() * 20));

        // จุดล่าสุดในตาราง (i === 0) ใช้ค่าจริง Real-time ล่าสุดจากเซนเซอร์ 100%
        if (i === 0) {
          indoorTemp = currentLive.temp1;
          outdoorTemp = currentLive.temp2;
          inverterTemp = currentLive.temp3;
          lux = currentLive.lux;
        }

        const dd = String(t.getDate()).padStart(2, '0');
        const mm = String(t.getMonth() + 1).padStart(2, '0');
        const yyyy = t.getFullYear();
        const dateStr = `${dd}/${mm}/${yyyy}`;

        const hh = String(t.getHours()).padStart(2, '0');
        const mi = String(t.getMinutes()).padStart(2, '0');
        const ss = String(t.getSeconds()).padStart(2, '0');
        const timeStr = `${hh}:${mi}:${ss}`;

        sampleList.push({
          timestamp: t.getTime(),
          iso: t.toISOString(),
          dateStr,
          timeStr,
          temp1: indoorTemp,
          temp2: outdoorTemp,
          temp3: inverterTemp,
          lux: Math.max(0, lux),
          source: 'sample',
        });
      }

      this.records = sampleList;
      this.lastLogTime = now;
      try { localStorage.setItem(this.LAST_LOG_KEY, String(now)); } catch(e){}

      if (this.db) {
        try {
          const tx = this.db.transaction([this.STORE_NAME], 'readwrite');
          const store = tx.objectStore(this.STORE_NAME);
          store.clear();
          sampleList.forEach((r) => store.put(r));
        } catch (e) {
          // ignore
        }
      }
      this.syncFallback();
      this.publishCloudHistory();

      this.render();
      if (!silent) {
        showToast('success', 'สร้างข้อมูลย้อนหลัง 30 วัน (1,440 รายการ) สำเร็จ!');
        addLog('info', '[ประวัติเซนเซอร์] สร้างข้อมูลย้อนหลัง 30 วัน (1,440 รายการ) เรียบร้อยแล้ว');
      }
    },

    async clearAllRecords() {
      if (!confirm('คุณแน่ใจหรือไม่ว่าต้องการล้างข้อมูลประวัติเซนเซอร์ทั้งหมด? การกระทำนี้ไม่สามารถย้อนกลับได้')) {
        return;
      }

      this.records = [];
      this.lastLogTime = 0;
      this.liveSamples = { temp1: [], temp2: [], temp3: [], lux: [], maxSamples: 2000 };
      try {
        localStorage.setItem(this.LAST_LOG_KEY, '0');
        localStorage.removeItem(this.STORAGE_KEY);
      } catch (e) {}

      if (this.db) {
        try {
          const tx = this.db.transaction([this.STORE_NAME], 'readwrite');
          const store = tx.objectStore(this.STORE_NAME);
          store.clear();
        } catch (e) {
          // ignore
        }
      }

      // Clear retained history on HiveMQ Cloud & notify peers
      if (state.mqttClient && state.mqttClient.connected) {
        try {
          const clearPayload = JSON.stringify({
            updatedAt: Date.now(),
            senderId: state.clientId,
            records: []
          });
          state.mqttClient.publish(CONFIG.topicHistorySync, clearPayload, { qos: 1, retain: true });
          state.mqttClient.publish(CONFIG.topicSync, JSON.stringify({ type: 'clear_sensor_history', senderId: state.clientId }), { qos: 1 });
        } catch (e) { }
      }

      this.render();
      this.updateCloudSyncStatus('synced', 'ล้างข้อมูลตรงกันทุกเครื่องแล้ว');
      showToast('info', 'ล้างข้อมูลประวัติเซนเซอร์เรียบร้อยแล้ว (ทุกเครื่องตรงกัน)');
      addLog('warning', '[ประวัติเซนเซอร์] ล้างข้อมูลประวัติทั้งหมดทั้งในเครื่องและบนคลาวด์แล้ว');
    },

    bindUI() {
      const snapBtn = document.getElementById('historySnapshotBtn');
      if (snapBtn) {
        snapBtn.addEventListener('click', () => this.logCurrentSnapshot('manual'));
      }

      const exportExcelBtn = document.getElementById('historyExportExcelBtn');
      if (exportExcelBtn) {
        exportExcelBtn.addEventListener('click', () => this.exportExcel());
      }

      const exportBtn = document.getElementById('historyExportCsvBtn');
      if (exportBtn) {
        exportBtn.addEventListener('click', () => this.exportCSV());
      }

      const sampleBtn = document.getElementById('historySampleDataBtn');
      if (sampleBtn) {
        sampleBtn.addEventListener('click', () => this.generateSample30DayData());
      }

      const clearBtn = document.getElementById('historyClearBtn');
      if (clearBtn) {
        clearBtn.addEventListener('click', () => this.clearAllRecords());
      }

      const rangePills = document.querySelectorAll('.history-pill');
      const customDatesPanel = document.getElementById('historyCustomDates');
      rangePills.forEach((pill) => {
        pill.addEventListener('click', () => {
          rangePills.forEach((p) => p.classList.remove('history-pill--active'));
          pill.classList.add('history-pill--active');
          const range = pill.getAttribute('data-range');
          this.currentRange = range;
          if (customDatesPanel) {
            customDatesPanel.style.display = range === 'custom' ? 'flex' : 'none';
          }
          if (range !== 'custom') {
            this.currentPage = 1;
            this.render();
          }
        });
      });

      const applyCustomBtn = document.getElementById('historyApplyCustomDateBtn');
      const startInp = document.getElementById('historyStartDate');
      const endInp = document.getElementById('historyEndDate');
      if (applyCustomBtn && startInp && endInp) {
        applyCustomBtn.addEventListener('click', () => {
          this.customStart = startInp.value;
          this.customEnd = endInp.value;
          this.currentPage = 1;
          this.render();
        });
      }

      const t1 = document.getElementById('toggleSensor1');
      const t2 = document.getElementById('toggleSensor2');
      const t3 = document.getElementById('toggleSensor3');
      const tLux = document.getElementById('toggleSensorLux');

      const handleToggle = () => {
        this.activeSensors = {
          s1: t1 ? t1.checked : true,
          s2: t2 ? t2.checked : true,
          s3: t3 ? t3.checked : true,
          lux: tLux ? tLux.checked : true,
        };
        this.render();
      };
      if (t1) t1.addEventListener('change', handleToggle);
      if (t2) t2.addEventListener('change', handleToggle);
      if (t3) t3.addEventListener('change', handleToggle);
      if (tLux) tLux.addEventListener('change', handleToggle);

      const chartTabs = document.querySelectorAll('.history-chart-tab');
      chartTabs.forEach((tab) => {
        tab.addEventListener('click', () => {
          chartTabs.forEach((t) => t.classList.remove('history-chart-tab--active'));
          tab.classList.add('history-chart-tab--active');
          this.chartMode = tab.getAttribute('data-mode') || 'all';
          this.renderChart(this.getFilteredRecords());
        });
      });

      const searchInp = document.getElementById('historySearchInput');
      if (searchInp) {
        searchInp.addEventListener('input', (e) => {
          this.searchQuery = e.target.value;
          this.currentPage = 1;
          this.renderTable(this.getFilteredRecords());
        });
      }

      const pageSizeSel = document.getElementById('historyPageSizeSelect');
      if (pageSizeSel) {
        pageSizeSel.addEventListener('change', (e) => {
          this.pageSize = parseInt(e.target.value, 10) || 25;
          this.currentPage = 1;
          this.renderTable(this.getFilteredRecords());
        });
      }

      const prevBtn = document.getElementById('historyPrevPageBtn');
      const nextBtn = document.getElementById('historyNextPageBtn');
      if (prevBtn) {
        prevBtn.addEventListener('click', () => {
          if (this.currentPage > 1) {
            this.currentPage--;
            this.renderTable(this.getFilteredRecords());
          }
        });
      }
      if (nextBtn) {
        nextBtn.addEventListener('click', () => {
          this.currentPage++;
          this.renderTable(this.getFilteredRecords());
        });
      }

      const canvasEl = document.getElementById('historyCanvas');
      const tooltipEl = document.getElementById('historyChartTooltip');
      if (canvasEl && tooltipEl) {
        this.canvas = canvasEl;
        this.ctx = canvasEl.getContext('2d');

        let pointerRaf = null;

        const handlePointerMove = (clientX, clientY) => {
          if (pointerRaf) return;
          pointerRaf = requestAnimationFrame(() => {
            pointerRaf = null;
            const filtered = this.getFilteredRecords();
            if (filtered.length === 0) return;

            const rect = canvasEl.getBoundingClientRect();
            const mouseX = clientX - rect.left;
            const padding = { top: 25, right: 65, bottom: 40, left: 55 };
            const plotW = rect.width - padding.left - padding.right;

            if (mouseX < padding.left || mouseX > padding.left + plotW) {
              if (this.hoverIndex !== -1) {
                this.hoverIndex = -1;
                tooltipEl.style.display = 'none';
                this.renderChart(filtered);
              }
              return;
            }

            const minTime = filtered[0].timestamp;
            const maxTime = filtered[filtered.length - 1].timestamp;
            const timeSpan = maxTime - minTime || 1;
            const targetTime = minTime + ((mouseX - padding.left) / plotW) * timeSpan;

            let closestIdx = 0;
            let minDiff = Infinity;
            for (let i = 0; i < filtered.length; i++) {
              const diff = Math.abs(filtered[i].timestamp - targetTime);
              if (diff < minDiff) {
                minDiff = diff;
                closestIdx = i;
              }
            }

            const changed = (this.hoverIndex !== closestIdx);
            this.hoverIndex = closestIdx;
            const pt = filtered[closestIdx];

            if (changed || tooltipEl.style.display !== 'block') {
              tooltipEl.innerHTML = `
                <div style="font-weight:600;margin-bottom:4px;border-bottom:1px solid rgba(255,255,255,0.2);padding-bottom:3px;">
                  📅 ${escapeHtml(pt.dateStr)} &nbsp;⏰ ${escapeHtml(pt.timeStr)}
                </div>
                ${this.activeSensors.s1 && pt.temp1 != null ? `<div style="color:#38bdf8;">● เซนเซอร์ 1 (Indoor): <strong>${Number(pt.temp1).toFixed(1)} °C</strong></div>` : ''}
                ${this.activeSensors.s2 && pt.temp2 != null ? `<div style="color:#fb923c;">● เซนเซอร์ 2 (Outdoor): <strong>${Number(pt.temp2).toFixed(1)} °C</strong></div>` : ''}
                ${this.activeSensors.s3 && pt.temp3 != null ? `<div style="color:#c084fc;">● เซนเซอร์ 3 (Inverter): <strong>${Number(pt.temp3).toFixed(1)} °C</strong></div>` : ''}
                ${this.activeSensors.lux && pt.lux != null ? `<div style="color:#facc15;">● ความเข้มแสง (Lux): <strong>${Number(pt.lux).toLocaleString()} lx</strong></div>` : ''}
                <div style="font-size:0.7rem;color:#94a3b8;margin-top:4px;">🏷️ บันทึก: ${pt.source === 'auto' ? 'อัตโนมัติ (30 นาที)' : 'บันทึกทันที'}</div>
              `;

              tooltipEl.style.display = 'block';
              this.renderChart(filtered);
            }

            const tooltipWidth = tooltipEl.offsetWidth || 180;
            let leftPos = mouseX + 12;
            if (leftPos + tooltipWidth > rect.width) {
              leftPos = mouseX - tooltipWidth - 12;
            }
            tooltipEl.style.left = `${Math.max(8, leftPos)}px`;
            tooltipEl.style.top = '14px';
          });
        };

        canvasEl.addEventListener('mousemove', (e) => handlePointerMove(e.clientX, e.clientY));
        canvasEl.addEventListener('mouseleave', () => {
          this.hoverIndex = -1;
          tooltipEl.style.display = 'none';
          this.renderChart(this.getFilteredRecords());
        });

        canvasEl.addEventListener('touchmove', (e) => {
          if (e.touches && e.touches[0]) {
            handlePointerMove(e.touches[0].clientX, e.touches[0].clientY);
          }
        }, { passive: true });
        canvasEl.addEventListener('touchend', () => {
          this.hoverIndex = -1;
          tooltipEl.style.display = 'none';
          this.renderChart(this.getFilteredRecords());
        });

        let resizeTimer = null;
        window.addEventListener('resize', () => {
          clearTimeout(resizeTimer);
          resizeTimer = setTimeout(() => {
            this.renderChart(this.getFilteredRecords());
          }, 150);
        });
      }
    },
  };

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
