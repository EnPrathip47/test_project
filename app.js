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
    reconnectDelay: 3000,
    maxReconnectAttempts: 10,
    demoUpdateInterval: 2000,
  };

  // ── State ──
  const state = {
    mqttClient: null,
    connected: false,
    demoMode: false,
    demoTimer: null,
    reconnectTimer: null,
    reconnectAttempts: 0,
    acOn: false,
    targetTemp: 25,
    systemState: 'idle', // idle | ready | running | stopped | timeout
    scheduleMode: 'auto', // 'auto' | 'manual'
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
    acMode: 0,      // 0=AUTO, 1=COOL, 2=DRY, 3=FAN
    acFan: 0,       // 0=AUTO, 1=LOW, 2=MED, 3=HIGH
    esp32Online: false,
    plcOnline: false,
    mqttOnline: false,
    lastCommand: '',

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

    // Guide Modal
    headerGuideBtn: document.getElementById('headerGuideBtn'),
    guideModal: document.getElementById('guideModal'),
    guideModalClose: document.getElementById('guideModalClose'),
    guideModalBackdrop: document.getElementById('guideModalBackdrop'),

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

    // Auto/Manual Mode Toggle
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
    updateSystemState('idle');
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

    // If stop is before or equal to start, and same date (or no offDate): overnight schedule (+1 day)
    if (stop <= start && (!offDateStr || onIso === offIso)) {
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
    }
    updateClock();
    setInterval(updateClock, 1000);
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

    // Browser-side schedule trigger: When reaching start time (Auto start working immediately)
    if (state.systemState === 'ready' || state.systemState === 'idle') {
      if (now >= start && now < stop) {
        updateSystemState('running');
        sendMqttPayload(1, getValidTargetTemp(), state.acMode, state.acFan);
        addLog('success', `[Schedule] ถึงเวลาเริ่มทำงาน (${onTimeVal}) -> ระบบเริ่มทำงานอัตโนมัติ (ส่งคำสั่งเปิดแอร์ไปยัง ESP32-S3)`);
        showToast('success', `ถึงเวลาเปิดแอร์แล้ว (${onTimeVal}) — ระบบเริ่มทำงานอัตโนมัติ`);
      }
    }
    // When running, reach off time -> timeout
    else if (state.systemState === 'running') {
      if (now >= stop) {
        updateSystemState('timeout');
        sendMqttPayload(0, getValidTargetTemp(), state.acMode, state.acFan);
        addLog('warning', `[Schedule] ครบเวลาเปิดแอร์ (${offTimeVal}) -> ส่งคำสั่งปิดแอร์ไปยัง ESP32-S3`);
        showToast('warning', `ทำงานครบเวลาแล้ว (${offTimeVal}) — ไฟแดงกระพริบ (กดรีเซทเพื่อเริ่มใหม่)`);
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
      updateScheduleInputsState();
    });
    DOM.offDate?.addEventListener('change', () => {
      validateDateNotPast(DOM.offDate);
      updateScheduleInputsState();
    });
    DOM.onTime?.addEventListener('input', updateScheduleInputsState);
    DOM.offTime?.addEventListener('input', updateScheduleInputsState);

    DOM.targetTemp?.addEventListener('change', () => {
      state.userModifiedTemp = true;
      getValidTargetTemp();
      updateMqttTempDisplay();
      saveSettings();
    });
    DOM.targetTemp?.addEventListener('input', () => {
      state.userModifiedTemp = true;
      updateMqttTempDisplay();
    });
    DOM.targetTemp?.addEventListener('blur', () => {
      getValidTargetTemp();
      updateMqttTempDisplay();
    });

    // Guide Modal Listeners
    DOM.headerGuideBtn?.addEventListener('click', openGuideModal);
    DOM.guideModalClose?.addEventListener('click', closeGuideModal);
    DOM.guideModalBackdrop?.addEventListener('click', closeGuideModal);

    // Temp +/- Controls
    DOM.tempMinusBtn?.addEventListener('click', () => adjustTempStep(-1));
    DOM.tempPlusBtn?.addEventListener('click', () => adjustTempStep(1));

    // Temp Presets Chips
    document.querySelectorAll('.temp-chip').forEach((chip) => {
      chip.addEventListener('click', (e) => {
        const val = parseFloat(e.currentTarget.getAttribute('data-temp'));
        if (!isNaN(val)) setTargetTemp(val, true);
      });
    });

    // MQTT Remote Control Events
    DOM.powerBtnOn?.addEventListener('click', () => setMqttPower(1, true));
    DOM.powerBtnOff?.addEventListener('click', () => setMqttPower(0, true));

    DOM.modeSelect?.addEventListener('change', () => {
      state.acMode = parseInt(DOM.modeSelect.value, 10);
      state.userModifiedMode = true;
    });

    DOM.fanSelect?.addEventListener('change', () => {
      state.acFan = parseInt(DOM.fanSelect.value, 10);
      state.userModifiedFan = true;
    });

    DOM.btnSendMqtt?.addEventListener('click', sendMqttCommandFromUI);

    // Auto/Manual Mode Toggle
    DOM.modeAutoBtn?.addEventListener('click', () => setScheduleMode('auto'));
    DOM.modeManualBtn?.addEventListener('click', () => setScheduleMode('manual'));
  }

  function openGuideModal() {
    if (DOM.guideModal) {
      DOM.guideModal.classList.add('guide-modal--open');
      DOM.guideModal.setAttribute('aria-hidden', 'false');
    }
  }

  function closeGuideModal() {
    if (DOM.guideModal) {
      DOM.guideModal.classList.remove('guide-modal--open');
      DOM.guideModal.setAttribute('aria-hidden', 'true');
    }
  }

  function adjustTempStep(delta) {
    const current = parseFloat(DOM.targetTemp?.value) || 25;
    const nextVal = Math.min(27, Math.max(18, current + delta));
    setTargetTemp(nextVal, true);
  }

  function setTargetTemp(val, isUserAction = false) {
    const validVal = Math.min(27, Math.max(18, val));
    if (DOM.targetTemp) DOM.targetTemp.value = validVal;
    state.targetTemp = validVal;
    if (isUserAction) {
      state.userModifiedTemp = true;
    }
    
    document.querySelectorAll('.temp-chip').forEach((chip) => {
      const chipVal = parseFloat(chip.getAttribute('data-temp'));
      chip.classList.toggle('temp-chip--active', chipVal === validVal);
    });

    saveSettings();
    updateMqttTempDisplay();
    showToast('info', `ตั้งอุณหภูมิแอร์เป็น ${validVal}°C`);
  }

  // Validate Target Temperature (Must be between 18°C and 27°C)
  function getValidTargetTemp() {
    const rawVal = parseFloat(DOM.targetTemp?.value);
    if (isNaN(rawVal) || rawVal < 18 || rawVal > 27) {
      let valid = rawVal < 18 ? 18 : 27;
      if (isNaN(rawVal)) valid = 25;
      if (DOM.targetTemp) DOM.targetTemp.value = valid;
      state.targetTemp = valid;
      showToast('warning', 'อุณหภูมิต้องอยู่ระหว่าง 18°C ถึง 27°C (ปรับเป็นค่าที่ถูกต้องให้อัตโนมัติ)');
      return valid;
    }
    state.targetTemp = rawVal;
    return rawVal;
  }

  function updateScheduleInputsState() {
    // In manual mode, editing input fields invalidates previous saved schedule
    if (state.scheduleMode === 'manual') {
      state.schedule.enabled = false;
      if (state.systemState === 'ready' || state.systemState === 'idle') {
        updateSystemState('idle');
      } else {
        updateControlButtons();
      }
    } else {
      updateControlButtons();
    }
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
      } catch (e) {}
    }
    if (DOM.mqttHostInput) DOM.mqttHostInput.value = CONFIG.mqttHost;
    if (DOM.mqttUsernameInput) DOM.mqttUsernameInput.value = CONFIG.mqttUsername;
    if (DOM.mqttPasswordInput) DOM.mqttPasswordInput.value = CONFIG.mqttPassword;

    const saved = localStorage.getItem('airCandySettings') || localStorage.getItem('processAirSettings');
    if (saved) {
      try {
        const settings = JSON.parse(saved);
        if (settings.targetTemp != null) {
          const temp = parseFloat(settings.targetTemp);
          if (!isNaN(temp) && temp >= 18 && temp <= 27) {
            state.targetTemp = temp;
            if (DOM.targetTemp) DOM.targetTemp.value = temp;
          }
        }
        // Load schedule mode preference
        if (settings.scheduleMode === 'auto' || settings.scheduleMode === 'manual') {
          state.scheduleMode = settings.scheduleMode;
        }
      } catch (e) {
        console.warn('Failed to load settings:', e);
      }
    }

    state.schedule.enabled = false;
    state.schedule.onTime = '';
    state.schedule.offTime = '';
    state.schedule.onDate = '';
    state.schedule.offDate = '';
    if (DOM.onTime) DOM.onTime.value = '';
    if (DOM.offTime) DOM.offTime.value = '';
    if (DOM.onDate) DOM.onDate.value = '';
    if (DOM.offDate) DOM.offDate.value = '';
    if (DOM.targetTemp && !DOM.targetTemp.value) DOM.targetTemp.value = state.targetTemp || 25;
  }

  function saveSettings() {
    const settings = {
      targetTemp: state.targetTemp,
      scheduleMode: state.scheduleMode,
    };
    localStorage.setItem('airCandySettings', JSON.stringify(settings));
  }

  // ============================================================
  //  AUTO / MANUAL SCHEDULE MODE
  // ============================================================

  function setScheduleMode(mode) {
    if (state.systemState === 'running' || state.systemState === 'stopped' || state.systemState === 'timeout') {
      showToast('warning', 'ไม่สามารถเปลี่ยนโหมดได้ขณะทำงานหรือล็อกอยู่ — กดรีเซทก่อน');
      return;
    }
    state.scheduleMode = mode;
    applyScheduleMode(mode);
    saveSettings();

    if (mode === 'auto') {
      showToast('info', '🔄 เปลี่ยนเป็นโหมด AUTO — เวลาฟิกซ์ 08:00-17:00 ทุกวัน');
      addLog('info', '[Mode] เปลี่ยนเป็น AUTO MODE (08:00-17:00 ทุกวัน)');
    } else {
      showToast('info', '🛠️ เปลี่ยนเป็นโหมด MANUAL — ปรับวันเวลาได้อิสระ');
      addLog('info', '[Mode] เปลี่ยนเป็น MANUAL MODE (ปรับวันเวลาได้)');
    }
  }

  function applyScheduleMode(mode) {
    const isAuto = (mode === 'auto');
    const toggle = DOM.modeToggleBar?.querySelector('.mode-toggle');

    // Toggle button active states
    if (DOM.modeAutoBtn) {
      DOM.modeAutoBtn.classList.toggle('mode-toggle__btn--active', isAuto);
    }
    if (DOM.modeManualBtn) {
      DOM.modeManualBtn.classList.toggle('mode-toggle__btn--active', !isAuto);
    }

    // Slider animation
    if (toggle) {
      toggle.classList.toggle('mode-toggle--manual', !isAuto);
    }

    // Mode info badge
    if (DOM.modeInfoBadge) {
      DOM.modeInfoBadge.textContent = isAuto ? '🔄 AUTO MODE' : '🛠️ MANUAL MODE';
      DOM.modeInfoBadge.className = 'mode-info__badge ' + (isAuto ? 'mode-info__badge--auto' : 'mode-info__badge--manual');
    }
    if (DOM.modeInfoDesc) {
      DOM.modeInfoDesc.textContent = isAuto
        ? 'ทำงานทุกวัน 08:00 - 17:00 | ปรับได้เฉพาะอุณหภูมิ'
        : 'ปรับวันที่ เวลา และอุณหภูมิได้อิสระ';
    }

    // Lock overlays
    if (DOM.onGroupLock) {
      DOM.onGroupLock.classList.toggle('schedule-group__lock--hidden', !isAuto);
    }
    if (DOM.offGroupLock) {
      DOM.offGroupLock.classList.toggle('schedule-group__lock--hidden', !isAuto);
    }

    // Disable/enable date & time inputs in auto mode
    if (DOM.onDate) DOM.onDate.disabled = isAuto;
    if (DOM.offDate) DOM.offDate.disabled = isAuto;
    if (DOM.onTime) DOM.onTime.disabled = isAuto;
    if (DOM.offTime) DOM.offTime.disabled = isAuto;

    // In auto mode, set fixed values
    if (isAuto) {
      if (DOM.onTime) DOM.onTime.value = '08:00';
      if (DOM.offTime) DOM.offTime.value = '17:00';
      // Use today's date
      const todayIso = getTodayIso();
      if (DOM.onDate) DOM.onDate.value = todayIso;
      if (DOM.offDate) DOM.offDate.value = todayIso;
    }

    // Refresh action buttons based on selected mode
    updateControlButtons();
  }

  // ============================================================
  //  HIVEMQ CLOUD MQTT OVER WEBSOCKET (WSS PORT 8884)
  // ============================================================

  function connectMqttBroker() {
    if (state.demoMode) stopDemo();
    if (state.mqttClient) {
      try { state.mqttClient.end(true); } catch(e){}
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
    } catch(e) {}

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

        addLog('success', 'เชื่อมต่อ HiveMQ Cloud MQTT Over WSS สำเร็จ!');
        showToast('success', 'เชื่อมต่อ HiveMQ MQTT สำเร็จ');
      });

      state.mqttClient.on('message', (topic, payload) => {
        try {
          const msgStr = payload.toString().trim();
          if (topic === CONFIG.topicAvailability) {
            const isOnline = (msgStr.toLowerCase() === 'online');
            state.esp32Online = isOnline;
            if (!isOnline) {
              state.plcOnline = false;
            }
            updateMqttStatusUI();
            addLog('info', `ESP32 Status: ${isOnline ? 'ONLINE' : 'OFFLINE'}`);
          } else if (topic === CONFIG.topicStatus) {
            const statusData = JSON.parse(msgStr);
            handleMqttStatus(statusData);
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
      try { state.mqttClient.end(true); } catch(e){}
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
  function sendMqttPayload(power, temp, mode, fan) {
    // Number type validation
    const p = Number(power);
    const t = Number(temp);
    const m = Number(mode);
    const f = Number(fan);

    const validationErrors = validatePayloadValues(p, t, m, f);
    if (validationErrors.length > 0) {
      if (DOM.mqttErrorMsg) DOM.mqttErrorMsg.textContent = validationErrors.join(', ');
      showToast('error', validationErrors[0]);
      return false;
    }
    if (DOM.mqttErrorMsg) DOM.mqttErrorMsg.textContent = '';

    const payloadObj = {
      power: p,
      temperature: t,
      mode: m,
      fan: f
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

  function sendMqttCommandFromUI() {
    const power = (state.systemState === 'running' || state.acOn) ? 1 : state.acPower;
    const temp = getValidTargetTemp();
    const mode = state.acMode;
    const fan = state.acFan;

    sendMqttPayload(power, temp, mode, fan);
  }

  // Handle incoming status payload from ESP32 (Actual State Readback)
  function handleMqttStatus(mqttData) {
    if (!mqttData || typeof mqttData !== 'object') return;

    // Actual State from ESP32 (Only sync controls if user hasn't modified them pending send)
    if (mqttData.power !== undefined && !state.userModifiedPower) {
      state.acPower = Number(mqttData.power);
      state.acOn = (state.acPower === 1);
      setMqttPower(state.acPower, false);
    }

    if (mqttData.temperature !== undefined && !state.userModifiedTemp) {
      const t = parseFloat(mqttData.temperature);
      if (!isNaN(t) && t >= 18 && t <= 27) {
        state.targetTemp = t;
        if (DOM.targetTemp && document.activeElement !== DOM.targetTemp) {
          DOM.targetTemp.value = t;
        }
        updateMqttTempDisplay();
      }
    }

    if (mqttData.mode !== undefined && !state.userModifiedMode) {
      state.acMode = Number(mqttData.mode);
      if (DOM.modeSelect && document.activeElement !== DOM.modeSelect) {
        DOM.modeSelect.value = state.acMode;
      }
    }

    if (mqttData.fan !== undefined && !state.userModifiedFan) {
      state.acFan = Number(mqttData.fan);
      if (DOM.fanSelect && document.activeElement !== DOM.fanSelect) {
        DOM.fanSelect.value = state.acFan;
      }
    }

    // Hardware Status Badges
    if (mqttData.esp32_online !== undefined) {
      state.esp32Online = Boolean(mqttData.esp32_online);
    }
    if (mqttData.plc_online !== undefined) {
      state.plcOnline = Boolean(mqttData.plc_online);
    }

    // 3 Temperature Sensors Readback (D0-D2)
    if (mqttData.temp1 !== undefined) updateSensor(1, parseFloat(mqttData.temp1));
    if (mqttData.temp2 !== undefined) updateSensor(2, parseFloat(mqttData.temp2));
    if (mqttData.temp3 !== undefined) updateSensor(3, parseFloat(mqttData.temp3));
    if (mqttData.temp1 !== undefined || mqttData.temp2 !== undefined || mqttData.temp3 !== undefined) {
      updateTempBadge();
    }

    // PLC Status Lights (Y0: Green, Y1: Yellow, Y2: Red)
    if (mqttData.y0_green !== undefined || mqttData.y1_yellow !== undefined || mqttData.y2_red !== undefined) {
      if (mqttData.y2_red === 1) {
        updateSystemState('stopped');
      } else if (mqttData.y0_green === 1) {
        updateSystemState('running');
      } else if (mqttData.y1_yellow === 1) {
        updateSystemState('idle');
      }
    } else {
      // Machine State UI Sync Fallback
      if (mqttData.machine_state === 'running' || state.acPower === 1) {
        if (state.systemState !== 'stopped' && state.systemState !== 'timeout') {
          updateSystemState('running');
        }
      } else if (mqttData.machine_state === 'stopped' && state.systemState === 'running') {
        updateSystemState('idle');
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
        DOM.scheduleStatusTag.textContent = `⚡ ตั้งค่าล่วงหน้าแล้ว — ไฟเขียวกระพริบ 1s${timeText}`;
        DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--ready';
      } else {
        DOM.scheduleStatusTag.textContent = '⚠️ Step 1: รอตั้งเวลา (ไฟเหลืองกระพริบ 1s)';
        DOM.scheduleStatusTag.className = 'schedule-status-tag schedule-status-tag--pending';
      }
    }

    switch (nextState) {
      case 'ready':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ (ตั้งเวลาแล้ว)');
        setLight(DOM.lightGreen, DOM.stateGreen, 'green-blink', 'READY', '⚡ กระพริบ 1s: รอถึงเวลาเริ่ม/รอกดสตาร์ท');
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
          DOM.currentStateBadge.textContent = `Step 2: READY (เขียวกระพริบ 1s)${timeText}`;
          DOM.currentStateBadge.className = 'state-badge state-badge--ready';
        }
        break;

      case 'running':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ');
        setLight(DOM.lightGreen, DOM.stateGreen, 'green-solid', 'RUNNING', '🟢 ติดค้าง: แอร์กำลังทำงาน');
        DOM.flowRunning?.classList.add('state-flow__step--active');
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'Step 3: RUNNING (เขียวติดค้าง)';
          DOM.currentStateBadge.className = 'state-badge state-badge--running';
        }
        break;

      case 'timeout':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightRed, DOM.stateRed, 'red-blink', 'TIMEOUT', '⚡ กระพริบ 1s: ครบเวลาทำงาน (ต้องกดรีเซท)');
        DOM.flowStopped?.classList.add('state-flow__step--active');
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'Step 4: TIMEOUT (แดงกระพริบ - กดรีเซท)';
          DOM.currentStateBadge.className = 'state-badge state-badge--stopped';
        }
        break;

      case 'stopped':
        setLight(DOM.lightYellow, DOM.stateYellow, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightGreen, DOM.stateGreen, null, 'OFF', '❌ ดับ (ล็อกระบบ)');
        setLight(DOM.lightRed, DOM.stateRed, 'red-solid', 'STOPPED', '🔴 ติดค้าง: กด Stop (ต้องกดรีเซท)');
        DOM.flowStopped?.classList.add('state-flow__step--active');
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'Case 2: STOPPED (แดงติดค้าง - กดรีเซท)';
          DOM.currentStateBadge.className = 'state-badge state-badge--stopped';
        }
        break;

      case 'idle':
      default:
        state.systemState = 'idle';
        DOM.flowIdle?.classList.add('state-flow__step--active');
        const idleDot = DOM.flowIdle?.querySelector('.state-flow__dot');

        setLight(DOM.lightYellow, DOM.stateYellow, 'amber-blink', 'IDLE', '⚡ กระพริบ 1s: ยังไม่ตั้งเวลา/รีเซท');
        if (idleDot) idleDot.className = 'state-flow__dot state-flow__dot--amber-blink';
        if (DOM.currentStateBadge) {
          DOM.currentStateBadge.textContent = 'Step 1: IDLE (เหลืองกระพริบ 1s)';
          DOM.currentStateBadge.className = 'state-badge state-badge--amber-blink';
        }
        break;
    }

    updateControlButtons();
  }

  function updateControlButtons() {
    const isAuto = (state.scheduleMode === 'auto');
    const hasConfiguredSchedule = isAuto || isScheduleSet();

    if (state.systemState === 'stopped' || state.systemState === 'timeout') {
      if (DOM.btnSave) {
        DOM.btnSave.disabled = true;
        if (DOM.btnSaveHint) DOM.btnSaveHint.textContent = '';
      }
      if (DOM.btnStart) {
        DOM.btnStart.disabled = true;
        if (DOM.btnStartHint) DOM.btnStartHint.textContent = 'ระบบถูกล็อก (กดรีเซท)';
      }
      if (DOM.btnStop) DOM.btnStop.disabled = true;
      if (DOM.btnReset) DOM.btnReset.disabled = false;
      return;
    }

    // ปุ่มบันทึกค่า: ในโหมด AUTO จะปิดการใช้งาน (ไม่จำเป็นต้องกด เพราะเวลาฟิกซ์ 08:00-17:00 ไว้แล้ว)
    if (DOM.btnSave) {
      DOM.btnSave.disabled = isAuto;
      if (DOM.btnSaveHint) {
        DOM.btnSaveHint.textContent = isAuto ? 'โหมด AUTO ฟิกซ์เวลาแล้ว' : '';
      }
      DOM.btnSave.title = isAuto ? 'โหมด AUTO ฟิกซ์เวลา 08:00 - 17:00 อัตโนมัติ (ไม่ต้องกดบันทึกค่า)' : 'บันทึกการตั้งเวลา';
    }

    // ปุ่มเริ่มทำงาน:
    // - ในโหมด AUTO: กดเริ่มทำงานได้ทันที (เมื่อไม่ได้ running)
    // - ในโหมด MANUAL: ต้องกดบันทึกค่าก่อน
    const canStart = isAuto
      ? (state.systemState !== 'running')
      : (hasConfiguredSchedule && state.systemState !== 'running');

    if (DOM.btnStart) {
      DOM.btnStart.disabled = !canStart;
      if (DOM.btnStartHint) {
        if (isAuto) {
          DOM.btnStartHint.textContent = (state.systemState === 'running') ? 'กำลังทำงาน' : 'กดเพื่อเริ่มทำงาน';
        } else {
          DOM.btnStartHint.textContent = hasConfiguredSchedule ? 'พร้อมเริ่มทำงาน' : 'ต้องกดบันทึกค่าก่อน';
        }
      }
      DOM.btnStart.title = isAuto
        ? 'เริ่มการทำงานในโหมด AUTO (08:00 - 17:00)'
        : (hasConfiguredSchedule ? 'เริ่มการทำงาน' : 'กรุณากดบันทึกเวลาก่อนกดเริ่มทำงาน');
    }

    // ปุ่มหยุดทำงาน: ใช้งานได้เมื่อระบบกำลัง running
    if (DOM.btnStop) {
      DOM.btnStop.disabled = (state.systemState !== 'running');
    }

    // ปุ่มรีเซท: สามารถกดรีเซทระบบได้เสมอ
    if (DOM.btnReset) {
      DOM.btnReset.disabled = false;
    }
  }

  function setMqttPower(val, isUserAction = false) {
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
      showToast('error', 'กรุณากำหนดเวลาเปิดและเวลาปิดแอร์');
      return;
    }

    const onIso = parseThaiDateToIso(onDateVal);
    const offIso = parseThaiDateToIso(offDateVal);
    if (offIso < onIso) {
      showToast('error', 'วันที่ปิดแอร์ต้องไม่เกิดขึ้นก่อนวันที่เปิดแอร์');
      return;
    }

    state.schedule.onDate = onDateVal;
    state.schedule.onTime = onTimeVal;
    state.schedule.offDate = offDateVal;
    state.schedule.offTime = offTimeVal;
    state.schedule.enabled = true;

    saveSettings();

    const modeLabel = isAutoMode ? '[AUTO]' : '[MANUAL]';
    const now = new Date();
    const { start, stop } = getScheduleRange(onDateVal, onTimeVal, offDateVal, offTimeVal);

    if (start && stop) {
      // บังคับการตั้งเวลาปิดให้มากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป (>= 60,000 ms)
      const diffMs = stop.getTime() - start.getTime();
      if (diffMs < 60 * 1000) {
        showToast('error', 'การตั้งเวลาปิดแอร์ ต้องตั้งให้มากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป');
        return;
      }

      if (now < start) {
        state.acOn = false;
        updateSystemState('ready');
        const isFutureDate = (onIso > todayIso);
        const displayWait = isFutureDate ? `${formatDisplayDate(onDateVal)} ${onTimeVal}` : onTimeVal;
        addLog('success', `${modeLabel} ตั้งเวลาล่วงหน้าสำเร็จ: ${formatDisplayDate(onDateVal)} ${onTimeVal} - ${formatDisplayDate(offDateVal)} ${offTimeVal} (${targetTemp}°C) (ไฟเขียวกระพริบ รอถึงเวลาเริ่ม)`);
        showToast('success', `${modeLabel} ตั้งเวลาล่วงหน้าสำเร็จ — อุณหภูมิ ${targetTemp}°C (รอถึงเวลา ${displayWait})`);
      } else if (now >= start && now < stop) {
        state.acOn = true;
        updateSystemState('running');
        sendMqttPayload(1, targetTemp, state.acMode, state.acFan);
        addLog('success', `${modeLabel} บันทึกเวลาสำเร็จ — ถึงช่วงเวลาทำงานแล้ว (${onTimeVal}) ตั้งค่า ${targetTemp}°C เปิดแอร์`);
        showToast('success', `${modeLabel} บันทึกเวลาสำเร็จ — เปิดแอร์ ${targetTemp}°C`);
      } else {
        if (isAutoMode) {
          // Auto mode: time already passed today, prepare for tomorrow
          showToast('info', `${modeLabel} เวลา 17:00 ผ่านมาแล้ววันนี้ — ระบบจะทำงานอัตโนมัติ 08:00-17:00 พรุ่งนี้`);
          state.acOn = false;
          updateSystemState('ready');
          addLog('info', `${modeLabel} เลยเวลา 17:00 แล้ว — ตั้งรอวันถัดไป 08:00-17:00`);
        } else {
          showToast('warning', `เวลาที่ตั้งไว้ (${offTimeVal}) ผ่านมาแล้ว กรุณากำหนดเวลาใหม่`);
          state.schedule.enabled = false;
          updateSystemState('idle');
        }
      }
    }
  }

  function startAC() {
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
      onDateVal  = state.schedule.onDate  || DOM.onDate?.value  || todayIso;
      offDateVal = state.schedule.offDate || DOM.offDate?.value || onDateVal;
      onTimeVal  = state.schedule.onTime  || DOM.onTime?.value;
      offTimeVal = state.schedule.offTime || DOM.offTime?.value;
    }

    const targetTemp = getValidTargetTemp();

    const finalOnDate  = onDateVal  || todayIso;
    const finalOffDate = offDateVal || finalOnDate;

    const onIso = parseThaiDateToIso(finalOnDate);
    const offIso = parseThaiDateToIso(finalOffDate);
    if (offIso < onIso) {
      showToast('error', 'วันที่ปิดแอร์ต้องไม่เกิดขึ้นก่อนวันที่เปิดแอร์');
      return;
    }

    state.schedule.onDate  = finalOnDate;
    state.schedule.onTime  = onTimeVal;
    state.schedule.offDate = finalOffDate;
    state.schedule.offTime = offTimeVal;
    state.schedule.enabled = true;

    saveSettings();

    const modeLabel = isAutoMode ? '[AUTO]' : '[MANUAL]';
    const now = new Date();
    const { start, stop } = getScheduleRange(finalOnDate, onTimeVal, finalOffDate, offTimeVal);

    if (start && stop) {
      // บังคับการตั้งเวลาปิดให้มากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป (>= 60,000 ms)
      const diffMs = stop.getTime() - start.getTime();
      if (diffMs < 60 * 1000) {
        showToast('error', 'การตั้งเวลาปิดแอร์ ต้องตั้งให้มากกว่าเวลาเปิดอย่างน้อย 1 นาทีขึ้นไป');
        return;
      }

      if (now < start) {
        state.acOn = false;
        updateSystemState('ready');
        const isFutureDate = (onIso > todayIso);
        const displayWait = isFutureDate ? `${formatDisplayDate(finalOnDate)} ${onTimeVal}` : onTimeVal;
        addLog('info', `${modeLabel} กดเริ่มทำงาน — ยังไม่ถึงเวลา (${displayWait}) ตั้งค่า ${targetTemp}°C ไฟเขียวกระพริบรอจนถึงเวลาเริ่ม`);
        showToast('info', `${modeLabel} กดเริ่มทำงานแล้ว — ตั้งอุณหภูมิ ${targetTemp}°C (รอถึงเวลา ${displayWait})`);
      } else if (now >= start && now < stop) {
        state.acOn = true;
        updateSystemState('running');
        sendMqttPayload(1, targetTemp, state.acMode, state.acFan);
        addLog('success', `${modeLabel} เริ่มทำงาน — ถึงเวลาเปิดแอร์แล้ว (${onTimeVal}) ตั้งค่า ${targetTemp}°C (ส่งคำสั่งไป ESP32)`);
        showToast('success', `${modeLabel} เริ่มทำงานแล้ว — เปิดแอร์ ${targetTemp}°C`);
      } else {
        if (isAutoMode) {
          showToast('info', `${modeLabel} เวลา 17:00 ผ่านมาแล้ววันนี้ — รอทำงานอัตโนมัติ 08:00-17:00 พรุ่งนี้`);
          state.acOn = false;
          updateSystemState('ready');
        } else {
          showToast('warning', `เวลาที่ตั้งไว้ (${offTimeVal}) ผ่านมาแล้ว กรุณากำหนดเวลาใหม่`);
          state.schedule.enabled = false;
          updateSystemState('idle');
        }
      }
    }
  }

  function stopAC() {
    state.acOn = false;
    sendMqttPayload(0, getValidTargetTemp(), state.acMode, state.acFan);
    updateSystemState('stopped');
    addLog('warning', 'กดหยุดการทำงาน — ส่งคำสั่งปิดแอร์ไปยัง ESP32-S3 (ไฟแดงติดค้าง)');
    showToast('warning', 'หยุดทำงานแล้ว — ไฟแดงติดค้าง (ต้องกดรีเซท)');
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

    sendMqttPayload(0, getValidTargetTemp(), state.acMode, state.acFan);
    saveSettings();

    // Re-apply schedule mode (restore auto mode lock overlays if needed)
    applyScheduleMode(state.scheduleMode);

    updateSystemState('idle');
    addLog('info', 'รีเซทระบบเรียบร้อย (กลับสู่ Step 1 IDLE)');
    showToast('success', 'รีเซทระบบเรียบร้อย');
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
  //  TOAST NOTIFICATIONS
  // ============================================================

  function showToast(type, message) {
    if (!DOM.toastContainer) return;
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

    setTimeout(() => {
      toast.classList.add('toast--removing');
      setTimeout(() => toast.remove(), 300);
    }, 3500);
  }

  // ── Utilities ──
  function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  // ── Start ──
  document.addEventListener('DOMContentLoaded', init);
})();
