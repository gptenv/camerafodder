const $ = (id) => document.getElementById(id);
const state = {
  session: JSON.parse(localStorage.getItem('cameraFodderSession') || 'null'),
  room: null,
  ws: null,
  localStream: null,
  cameraStream: null,
  screenStream: null,
  peers: new Map(),
  participants: new Map(),
  mediaByPeer: new Map(),
  roomEpoch: 0,
  joinRequests: { incoming: [], outgoing: [] },
  pendingRoomId: roomIdFromLocation(),
  layoutMode: localStorage.getItem('cameraFodderLayout') || 'grid',
  activeVideoId: 'local',
  activePanel: 'chat',
  media: {
    audioEnabled: true,
    videoEnabled: true,
    screenSharing: false,
    audioDeviceId: localStorage.getItem('cameraFodderAudioDevice') || '',
    videoDeviceId: localStorage.getItem('cameraFodderVideoDevice') || '',
    mirrorSelf: localStorage.getItem('cameraFodderMirrorSelf') !== 'false',
    compactTiles: localStorage.getItem('cameraFodderCompactTiles') === 'true',
  },
};

const rtcConfig = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

function roomIdFromLocation() {
  const [, section, roomId] = location.pathname.match(/^\/(room)\/([^/?#]+)/) || [];
  return section === 'room' ? roomId : new URLSearchParams(location.search).get('room');
}

function roomPath(roomId) {
  return `/room/${roomId}`;
}

function showLobbyView() {
  document.body.classList.remove('in-room', 'in-invite');
  $('lobbyView').classList.remove('hidden');
  $('inviteView').classList.add('hidden');
  $('roomPanel').classList.add('hidden');
}

function showInviteView(roomId, replace = false) {
  state.pendingRoomId = roomId;
  document.body.classList.remove('in-room');
  document.body.classList.add('in-invite');
  $('lobbyView').classList.add('hidden');
  $('inviteView').classList.remove('hidden');
  $('roomPanel').classList.add('hidden');
  const nextPath = roomPath(roomId);
  if (location.pathname !== nextPath) {
    history[replace ? 'replaceState' : 'pushState']({}, '', nextPath);
  }
  renderInviteState();
}

function showRoomView(roomId, replace = false) {
  document.body.classList.remove('in-invite');
  document.body.classList.add('in-room');
  $('lobbyView').classList.add('hidden');
  $('inviteView').classList.add('hidden');
  $('roomPanel').classList.remove('hidden');
  const nextPath = roomPath(roomId);
  if (location.pathname !== nextPath) {
    history[replace ? 'replaceState' : 'pushState']({}, '', nextPath);
  }
}

function setSession(session) {
  state.session = session;
  localStorage.setItem('cameraFodderSession', JSON.stringify(session));
  $('connectionDot').classList.add('on');
  $('sessionName').textContent = session.display_name;
  $('sessionId').textContent = session.id;
  $('logoutButton').classList.remove('hidden');
  $('authStatus').textContent = `Signed in as ${session.display_name}.`;
  $('inviteConnectionDot').classList.add('on');
  $('inviteSessionName').textContent = session.display_name;
  $('inviteSessionId').textContent = session.id;
  $('inviteLogoutButton').classList.remove('hidden');
  refreshJoinRequests().catch(console.warn);
  if (state.pendingRoomId && !state.room) {
    renderInviteState();
  }
}

function clearSession(message = 'Use guest mode, or create/sign into a local account.') {
  state.session = null;
  localStorage.removeItem('cameraFodderSession');
  $('connectionDot').classList.remove('on');
  $('sessionName').textContent = 'Not signed in';
  $('sessionId').textContent = 'Use guest mode or local auth to begin.';
  $('logoutButton').classList.add('hidden');
  $('authStatus').textContent = message;
  $('inviteConnectionDot').classList.remove('on');
  $('inviteSessionName').textContent = 'Not signed in';
  $('inviteSessionId').textContent = 'Start a guest or local account session first.';
  $('inviteLogoutButton').classList.add('hidden');
  state.joinRequests = { incoming: [], outgoing: [] };
  renderJoinRequests();
}

async function validateStoredSession() {
  if (!state.session?.id) return;
  try {
    const { session } = await api(`/api/auth/session/${state.session.id}`, null, 'GET');
    setSession(session);
  } catch (error) {
    clearSession(error.message || 'Session expired. Please sign in again.');
  }
}

function requireSession() {
  if (!state.session?.id) throw new Error('Start a guest or local account session first.');
}

async function api(path, body, method = 'POST') {
  const response = await fetch(path, {
    method,
    headers: body ? { 'content-type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || response.statusText);
  return payload;
}

function isActiveRoom(roomId, roomEpoch) {
  return state.room?.id === roomId && state.roomEpoch === roomEpoch;
}

function setRoomNotice(message) {
  if ($('roomNotice')) $('roomNotice').textContent = message;
}

function setLaunchStatus(message) {
  if ($('roomLaunchStatus')) $('roomLaunchStatus').textContent = message;
}

function currentPresence() {
  return {
    audio_enabled: state.media.audioEnabled,
    video_enabled: state.media.screenSharing || state.media.videoEnabled,
    screen_sharing: state.media.screenSharing,
  };
}

function broadcastPresence() {
  if (!state.room || !state.session) return;
  const presence = currentPresence();
  state.mediaByPeer.set(state.session.id, presence);
  updateVideoCardState('local', presence);
  renderParticipants();
  send({
    type: 'presence',
    audioEnabled: presence.audio_enabled,
    videoEnabled: presence.video_enabled,
    screenSharing: presence.screen_sharing,
  });
}

function mediaConstraints() {
  const audio = state.media.audioDeviceId
    ? { deviceId: { exact: state.media.audioDeviceId }, echoCancellation: true, noiseSuppression: true, autoGainControl: true }
    : { echoCancellation: true, noiseSuppression: true, autoGainControl: true };
  const video = state.media.videoDeviceId
    ? { deviceId: { exact: state.media.videoDeviceId }, width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 30, max: 30 } }
    : { width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 30, max: 30 } };
  return { audio, video };
}

async function restartCameraStream() {
  if (!navigator.mediaDevices?.getUserMedia) {
    throw new Error('This browser does not expose camera and microphone devices.');
  }
  const previous = state.cameraStream;
  const stream = await navigator.mediaDevices.getUserMedia(mediaConstraints());
  state.cameraStream = stream;
  applyMediaPreferences();
  rebuildLocalStream();
  previous?.getTracks().forEach((track) => track.stop());
  await refreshDeviceLists();
}

function applyMediaPreferences() {
  state.cameraStream?.getAudioTracks().forEach((track) => {
    track.enabled = state.media.audioEnabled;
  });
  state.cameraStream?.getVideoTracks().forEach((track) => {
    track.enabled = state.media.videoEnabled;
  });
}

function activeAudioTrack() {
  return state.cameraStream?.getAudioTracks()[0] || null;
}

function activeVideoTrack() {
  if (state.media.screenSharing) return state.screenStream?.getVideoTracks()[0] || null;
  return state.cameraStream?.getVideoTracks()[0] || null;
}

function composeLocalStream() {
  const tracks = [];
  const audioTrack = activeAudioTrack();
  const videoTrack = activeVideoTrack();
  if (audioTrack) tracks.push(audioTrack);
  if (videoTrack) tracks.push(videoTrack);
  return new MediaStream(tracks);
}

function replaceOutgoingTrack(kind, track) {
  state.peers.forEach(({ pc }) => {
    const sender = pc.getSenders().find((candidate) => candidate.track?.kind === kind);
    if (sender) sender.replaceTrack(track).catch(console.warn);
  });
}

function rebuildLocalStream() {
  if (!state.cameraStream && !state.screenStream) return;
  state.localStream = composeLocalStream();
  upsertVideo('local', state.localStream, `${state.session.display_name} (you)`, true);
  replaceOutgoingTrack('audio', activeAudioTrack());
  replaceOutgoingTrack('video', activeVideoTrack());
  updateControlStates();
  broadcastPresence();
}

async function ensureMedia(roomEpoch = state.roomEpoch) {
  const roomId = state.room?.id;
  if (!roomId || !isActiveRoom(roomId, roomEpoch)) {
    throw new Error('No active room needs media.');
  }
  if (!state.cameraStream) {
    setRoomNotice('Requesting camera and microphone access...');
    await restartCameraStream();
    if (!isActiveRoom(roomId, roomEpoch)) {
      stopLocalMedia();
      throw new Error('Room left before media started.');
    }
  }
  if (!state.localStream) rebuildLocalStream();
  return state.localStream;
}

function upsertVideo(id, stream, label, muted = false) {
  const card = ensureVideoCard(id, label);
  const video = card.querySelector('video');
  video.srcObject = stream;
  video.muted = muted;
  card.querySelector('.badge').textContent = label;
  card.querySelector('.tile-avatar').dataset.initials = initials(label);
  card.querySelector('.video-state').textContent = '';
  card.classList.remove('waiting');
  updateVideoCardState(id, id === 'local' ? currentPresence() : state.mediaByPeer.get(id));
  updateVideoLayout();
}

function ensureVideoCard(id, label) {
  let card = document.querySelector(`[data-video-id="${id}"]`);
  if (!card) {
    card = document.createElement('div');
    card.className = 'video-card waiting';
    card.dataset.videoId = id;
    if (id === 'local') card.classList.add('local-card');
    card.innerHTML = `
      <video autoplay playsinline></video>
      <span class="tile-scrim"></span>
      <span class="tile-avatar" data-initials=""></span>
      <span class="tile-top"><span class="tile-state">Connecting</span></span>
      <span class="tile-bottom"><span class="badge"></span><span class="tile-media">Live</span></span>
      <span class="video-state">Connecting...</span>
    `;
    card.onclick = () => {
      state.activeVideoId = id;
      setLayoutMode('focus');
    };
    $('videos').append(card);
  }
  card.querySelector('.badge').textContent = label;
  card.querySelector('.tile-avatar').dataset.initials = initials(label);
  if (id === 'local') card.classList.add('local-card');
  updateVideoCardState(id, id === 'local' ? currentPresence() : state.mediaByPeer.get(id));
  updateVideoLayout();
  return card;
}

function removeVideo(id) {
  document.querySelector(`[data-video-id="${id}"]`)?.remove();
  if (state.activeVideoId === id) state.activeVideoId = 'local';
  updateVideoLayout();
}

function stopLocalMedia() {
  state.cameraStream?.getTracks().forEach((track) => track.stop());
  state.screenStream?.getTracks().forEach((track) => track.stop());
  state.localStream?.getTracks().forEach((track) => track.stop());
  state.cameraStream = null;
  state.screenStream = null;
  state.localStream = null;
  state.media.screenSharing = false;
  const localVideo = document.querySelector('[data-video-id="local"] video');
  if (localVideo) localVideo.srcObject = null;
  updateControlStates();
}

function initials(label) {
  const clean = label.replace(/\(you\)/i, '').trim();
  const parts = clean.split(/\s+/).filter(Boolean).slice(0, 2);
  return (parts.map((part) => part[0]).join('') || 'CF').toUpperCase();
}

function updateVideoCardState(id, presence = {}) {
  const card = document.querySelector(`[data-video-id="${id}"]`);
  if (!card) return;
  const resolved = {
    audio_enabled: presence.audio_enabled !== false,
    video_enabled: presence.video_enabled !== false,
    screen_sharing: presence.screen_sharing === true,
  };
  card.classList.toggle('mic-muted', !resolved.audio_enabled);
  card.classList.toggle('camera-off', !resolved.video_enabled);
  card.classList.toggle('screen-sharing', resolved.screen_sharing);
  card.classList.toggle('mirror-self', id === 'local' && state.media.mirrorSelf);
  const media = card.querySelector('.tile-media');
  if (media) {
    if (resolved.screen_sharing) media.textContent = 'Sharing screen';
    else if (!resolved.audio_enabled && !resolved.video_enabled) media.textContent = 'Muted, camera off';
    else if (!resolved.audio_enabled) media.textContent = 'Muted';
    else if (!resolved.video_enabled) media.textContent = 'Camera off';
    else media.textContent = 'Live';
  }
  const tileState = card.querySelector('.tile-state');
  if (tileState) {
    tileState.textContent = resolved.screen_sharing
      ? 'Screen share'
      : resolved.video_enabled
        ? 'Connecting'
        : 'Camera off';
  }
}

function updateVideoLayout() {
  const videos = $('videos');
  if (!videos) return;
  const cards = [...videos.querySelectorAll('.video-card')];
  videos.dataset.count = String(cards.length);
  videos.classList.toggle('focus-layout', state.layoutMode === 'focus' && cards.length > 1);
  videos.classList.toggle('grid-layout', state.layoutMode !== 'focus' || cards.length <= 1);
  videos.classList.toggle('compact-layout', state.media.compactTiles);
  if (!cards.some((card) => card.dataset.videoId === state.activeVideoId)) {
    state.activeVideoId = cards[0]?.dataset.videoId || 'local';
  }
  for (const card of cards) {
    card.classList.toggle('is-active', state.layoutMode === 'focus' && card.dataset.videoId === state.activeVideoId);
  }
  updateControlStates();
}

function setLayoutMode(mode) {
  state.layoutMode = mode;
  localStorage.setItem('cameraFodderLayout', mode);
  $('gridLayoutButton')?.classList.toggle('active', mode === 'grid');
  $('focusLayoutButton')?.classList.toggle('active', mode === 'focus');
  updateVideoLayout();
}

function setActivePanel(panel) {
  state.activePanel = panel;
  const panels = {
    chat: ['chatPanel', 'tabChat'],
    people: ['peoplePanel', 'tabPeople'],
    requests: ['requestPanel', 'tabRequests'],
    settings: ['settingsPanel', 'tabSettings'],
  };
  for (const [name, [panelId, tabId]] of Object.entries(panels)) {
    $(panelId)?.classList.toggle('active', name === panel);
    $(tabId)?.classList.toggle('active', name === panel);
  }
}

function updateControlStates() {
  const mic = $('micButton');
  const camera = $('cameraButton');
  const screen = $('screenButton');
  const mirror = $('mirrorSelfToggle');
  const compact = $('compactTilesToggle');
  if (mic) {
    mic.textContent = state.media.audioEnabled ? 'Mic on' : 'Mic off';
    mic.setAttribute('aria-pressed', String(state.media.audioEnabled));
    mic.classList.toggle('is-off', !state.media.audioEnabled);
  }
  if (camera) {
    camera.textContent = state.media.videoEnabled ? 'Camera on' : 'Camera off';
    camera.setAttribute('aria-pressed', String(state.media.videoEnabled));
    camera.classList.toggle('is-off', !state.media.videoEnabled);
  }
  if (screen) {
    screen.textContent = state.media.screenSharing ? 'Stop sharing' : 'Share screen';
    screen.setAttribute('aria-pressed', String(state.media.screenSharing));
    screen.classList.toggle('is-off', state.media.screenSharing);
  }
  if (mirror) mirror.checked = state.media.mirrorSelf;
  if (compact) compact.checked = state.media.compactTiles;
}

function populateDeviceSelect(selectId, devices, selectedId, fallback) {
  const select = $(selectId);
  if (!select) return;
  const previous = selectedId || select.value;
  select.innerHTML = '';
  const defaultOption = document.createElement('option');
  defaultOption.value = '';
  defaultOption.textContent = fallback;
  select.append(defaultOption);
  devices.forEach((device, index) => {
    const option = document.createElement('option');
    option.value = device.deviceId;
    option.textContent = device.label || `${fallback} ${index + 1}`;
    select.append(option);
  });
  select.value = [...select.options].some((option) => option.value === previous) ? previous : '';
}

async function refreshDeviceLists() {
  if (!navigator.mediaDevices?.enumerateDevices) return;
  try {
    const devices = await navigator.mediaDevices.enumerateDevices();
    populateDeviceSelect('cameraSelect', devices.filter((device) => device.kind === 'videoinput'), state.media.videoDeviceId, 'Default camera');
    populateDeviceSelect('microphoneSelect', devices.filter((device) => device.kind === 'audioinput'), state.media.audioDeviceId, 'Default microphone');
    $('settingsStatus').textContent = 'Camera and microphone settings are ready.';
  } catch (error) {
    $('settingsStatus').textContent = error.message;
  }
}

async function changeDevice(kind, value) {
  if (kind === 'audio') {
    state.media.audioDeviceId = value;
    localStorage.setItem('cameraFodderAudioDevice', value);
  } else {
    state.media.videoDeviceId = value;
    localStorage.setItem('cameraFodderVideoDevice', value);
  }
  if (!state.room) return;
  try {
    setRoomNotice('Switching device...');
    await restartCameraStream();
    setRoomNotice('Device switched.');
  } catch (error) {
    setRoomNotice(error.message);
  }
}

async function toggleMic() {
  state.media.audioEnabled = !state.media.audioEnabled;
  applyMediaPreferences();
  updateControlStates();
  broadcastPresence();
}

async function toggleCamera() {
  state.media.videoEnabled = !state.media.videoEnabled;
  applyMediaPreferences();
  updateControlStates();
  broadcastPresence();
}

async function toggleScreenShare() {
  if (state.media.screenSharing) {
    stopScreenShare();
    return;
  }
  if (!navigator.mediaDevices?.getDisplayMedia) {
    setRoomNotice('Screen sharing is not available in this browser.');
    return;
  }
  try {
    const stream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
    state.screenStream = stream;
    state.media.screenSharing = true;
    const [track] = stream.getVideoTracks();
    if (track) {
      track.onended = () => {
        if (state.media.screenSharing) stopScreenShare();
      };
    }
    rebuildLocalStream();
    setRoomNotice('Screen sharing is live.');
  } catch (error) {
    setRoomNotice(error.message || 'Screen sharing was cancelled.');
  }
}

function stopScreenShare() {
  const stream = state.screenStream;
  state.screenStream = null;
  state.media.screenSharing = false;
  stream?.getTracks().forEach((track) => {
    track.onended = null;
    track.stop();
  });
  rebuildLocalStream();
  setRoomNotice('Screen sharing stopped.');
}

function peerIsPolite(peerId) {
  return state.session.id > peerId;
}

function participantLabel(peerId) {
  return state.participants.get(peerId)?.display_name || `Peer ${peerId.slice(0, 8)}`;
}

async function createPeer(peerId, roomEpoch = state.roomEpoch) {
  const roomId = state.room?.id;
  if (!roomId || !isActiveRoom(roomId, roomEpoch)) return null;
  if (state.peers.has(peerId)) return state.peers.get(peerId);
  const pc = new RTCPeerConnection(rtcConfig);
  const record = { pc, makingOffer: false, ignoreOffer: false, polite: peerIsPolite(peerId) };
  state.peers.set(peerId, record);
  ensureVideoCard(peerId, participantLabel(peerId));
  let stream;
  try {
    stream = await ensureMedia(roomEpoch);
  } catch (error) {
    pc.close();
    state.peers.delete(peerId);
    removeVideo(peerId);
    if (isActiveRoom(roomId, roomEpoch)) console.warn(error);
    return null;
  }
  if (!isActiveRoom(roomId, roomEpoch)) {
    pc.close();
    state.peers.delete(peerId);
    removeVideo(peerId);
    return null;
  }
  stream.getTracks().forEach((track) => pc.addTrack(track, stream));
  pc.ontrack = (event) => upsertVideo(peerId, event.streams[0], participantLabel(peerId));
  pc.onicecandidate = ({ candidate }) => candidate && send({ type: 'signal', to: peerId, payload: { candidate } });
  pc.onnegotiationneeded = async () => {
    try {
      record.makingOffer = true;
      await pc.setLocalDescription();
      send({ type: 'signal', to: peerId, payload: { description: pc.localDescription } });
    } catch (error) {
      console.warn('Unable to negotiate peer connection', error);
    } finally {
      record.makingOffer = false;
    }
  };
  return record;
}

async function handleSignal(from, payload, roomEpoch = state.roomEpoch) {
  const roomId = state.room?.id;
  if (!roomId || !isActiveRoom(roomId, roomEpoch)) return;
  const record = await createPeer(from, roomEpoch);
  if (!record || !isActiveRoom(roomId, roomEpoch)) return;
  const { pc } = record;
  if (payload.description) {
    const offerCollision = payload.description.type === 'offer' && (record.makingOffer || pc.signalingState !== 'stable');
    record.ignoreOffer = !record.polite && offerCollision;
    if (record.ignoreOffer) return;
    await pc.setRemoteDescription(payload.description);
    if (payload.description.type === 'offer') {
      await pc.setLocalDescription();
      send({ type: 'signal', to: from, payload: { description: pc.localDescription } });
    }
  } else if (payload.candidate) {
    try {
      await pc.addIceCandidate(payload.candidate);
    } catch (error) {
      if (!record.ignoreOffer) console.warn('Unable to add ICE candidate', error);
    }
  }
}

function send(event) {
  if (state.ws?.readyState === WebSocket.OPEN) state.ws.send(JSON.stringify(event));
}

async function joinRoom(room, options = {}) {
  requireSession();
  const roomEpoch = ++state.roomEpoch;
  state.room = room;
  state.mediaByPeer.clear();
  state.mediaByPeer.set(state.session.id, currentPresence());
  state.activeVideoId = 'local';
  showRoomView(room.id, options.replaceUrl);
  setLayoutMode(state.layoutMode);
  setActivePanel(state.activePanel);
  updateControlStates();
  setRoomNotice('Connecting to the room...');
  $('roomTitle').textContent = `Room ${room.id.slice(0, 8)}`;
  renderJoinRequests();
  if (room.participants) {
    rememberParticipants(room);
    renderRoomMeta(room);
  }
  else $('roomMeta').textContent = 'Joining shared room…';
  const wsUrl = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/rooms/${room.id}?session_id=${state.session.id}`;
  if (state.ws) state.ws.onclose = null;
  state.ws?.close();
  state.ws = new WebSocket(wsUrl);
  state.ws.onmessage = async ({ data }) => {
    if (!isActiveRoom(room.id, roomEpoch)) return;
    const event = JSON.parse(data);
    if (event.type === 'welcome') {
      if (!isActiveRoom(room.id, roomEpoch)) return;
      state.room = event.room;
      rememberParticipants(event.room);
      renderRoomMeta(event.room);
      refreshJoinRequests().catch(console.warn);
      try {
        await ensureMedia(roomEpoch);
      } catch (error) {
        if (isActiveRoom(room.id, roomEpoch)) console.warn(error);
        setRoomNotice(error.message);
        return;
      }
      if (!isActiveRoom(room.id, roomEpoch)) return;
      setRoomNotice('Connected. You can manage media and settings from the control dock.');
      broadcastPresence();
      for (const peer of event.room.participants.filter((p) => p.session_id !== state.session.id)) {
        if (!isActiveRoom(room.id, roomEpoch)) return;
        await createPeer(peer.session_id, roomEpoch);
      }
    }
    if (event.type === 'peerJoined') {
      if (!isActiveRoom(room.id, roomEpoch)) return;
      state.participants.set(event.peer.session_id, event.peer);
      renderParticipants();
      await createPeer(event.peer.session_id, roomEpoch);
      broadcastPresence();
    }
    if (event.type === 'peerLeft') {
      if (!isActiveRoom(room.id, roomEpoch)) return;
      state.peers.get(event.peer_id)?.pc.close();
      state.peers.delete(event.peer_id);
      state.participants.delete(event.peer_id);
      state.mediaByPeer.delete(event.peer_id);
      renderParticipants();
      removeVideo(event.peer_id);
    }
    if (event.type === 'signal') await handleSignal(event.from, event.payload, roomEpoch);
    if (event.type === 'presence') {
      state.mediaByPeer.set(event.from, {
        audio_enabled: event.audio_enabled ?? event.audioEnabled,
        video_enabled: event.video_enabled ?? event.videoEnabled,
        screen_sharing: event.screen_sharing ?? event.screenSharing,
      });
      updateVideoCardState(event.from, state.mediaByPeer.get(event.from));
      renderParticipants();
    }
    if (event.type === 'chat') appendChatMessage(event);
    if (event.type === 'roomUpdated') {
      if (!isActiveRoom(room.id, roomEpoch)) return;
      state.room = event.room;
      rememberParticipants(event.room);
      renderRoomMeta(event.room);
    }
    if (event.type === 'error') appendMessage(`Server: ${event.message}`);
  };
  state.ws.onclose = () => appendMessage('Disconnected from room.');
}

function renderRoomMeta(room) {
  const host = room.participants.find((participant) => participant.session_id === room.host_session);
  const canAddRandom = !room.host_controls_joiners || room.host_session === state.session?.id;
  $('roomMeta').textContent = `${room.waiting_for_random ? 'Waiting for a random match' : 'Matched'} · ${room.host_controls_joiners ? 'Host-managed room' : 'Open add-person controls'}`;
  $('roomStatusPill').textContent = room.waiting_for_random ? 'Waiting' : 'Live';
  $('roomHostPill').textContent = `Host: ${host?.display_name || 'pending'}`;
  $('roomCapacityPill').textContent = `${room.participants.length}/${room.max_size}`;
  $('addRandomButton').disabled = !state.room || !canAddRandom || room.participants.length >= room.max_size;
  $('addRandomButton').title = canAddRandom ? 'Call another random attendee into this room.' : 'Only the host can call in random attendees.';
}

function rememberParticipants(room) {
  state.participants.clear();
  for (const participant of room.participants || []) {
    state.participants.set(participant.session_id, participant);
    const videoId = participant.session_id === state.session?.id ? 'local' : participant.session_id;
    const card = document.querySelector(`[data-video-id="${videoId}"]`);
    if (card) card.querySelector('.badge').textContent = participant.session_id === state.session?.id
      ? `${participant.display_name} (you)`
      : participant.display_name;
    updateVideoCardState(videoId, participant.session_id === state.session?.id ? currentPresence() : state.mediaByPeer.get(participant.session_id));
  }
  renderParticipants();
}

function renderParticipants() {
  const list = $('participants');
  if (!list) return;
  list.innerHTML = '';
  const participants = [...state.participants.values()];
  if (!participants.length) {
    appendHint(list, 'No one is in the room yet.');
    return;
  }
  for (const participant of participants) {
    const item = document.createElement('div');
    item.className = 'participant';
    const main = document.createElement('div');
    main.className = 'participant-main';
    const name = document.createElement('strong');
    name.textContent = participant.session_id === state.session?.id
      ? `${participant.display_name} (you)`
      : participant.display_name;
    const status = document.createElement('small');
    status.textContent = participant.session_id === state.room?.host_session ? 'Host' : 'Guest';
    main.append(name, status);
    const badges = document.createElement('div');
    badges.className = 'participant-badges';
    const presence = participant.session_id === state.session?.id
      ? currentPresence()
      : state.mediaByPeer.get(participant.session_id);
    badges.append(mediaBadge(presence?.audio_enabled === false ? 'Muted' : 'Mic on', presence?.audio_enabled === false ? 'warning' : 'positive'));
    badges.append(mediaBadge(presence?.video_enabled === false ? 'Camera off' : 'Video on', presence?.video_enabled === false ? 'warning' : 'positive'));
    if (presence?.screen_sharing) badges.append(mediaBadge('Sharing', 'positive'));
    item.append(main, badges);
    list.append(item);
  }
}

function mediaBadge(text, tone) {
  const badge = document.createElement('span');
  badge.className = `mini-badge ${tone}`;
  badge.textContent = text;
  return badge;
}

function appendMessage(text) {
  const line = document.createElement('div');
  line.className = 'message-row';
  line.textContent = text;
  $('messages').append(line);
  $('messages').scrollTop = $('messages').scrollHeight;
}

function appendChatMessage(event) {
  const line = document.createElement('div');
  line.className = 'message-row';
  const meta = document.createElement('div');
  meta.className = 'message-meta';
  const sender = document.createElement('strong');
  sender.textContent = event.from === state.session?.id ? 'You' : (event.display_name || event.displayName || 'Guest');
  const time = document.createElement('span');
  time.textContent = new Date(event.at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  const text = document.createElement('div');
  text.className = 'message-text';
  text.textContent = event.text;
  meta.append(sender, time);
  line.append(meta, text);
  $('messages').append(line);
  $('messages').scrollTop = $('messages').scrollHeight;
}

function formatRoomId(roomId) {
  return `Room ${roomId.slice(0, 8)}`;
}

function setRequestStatus(message) {
  const text = message || (state.session
    ? 'Requests are checked quietly in the background.'
    : 'Start a session to send and accept requests.');
  $('requestStatus').textContent = text;
  if ($('directoryStatus')) $('directoryStatus').textContent = text;
  if (message && $('inviteStatus')) $('inviteStatus').textContent = message;
}

function outgoingRequestForRoom(roomId) {
  return state.joinRequests.outgoing.find((request) => request.room_id === roomId);
}

function renderInviteState() {
  const roomId = state.pendingRoomId || roomIdFromLocation();
  if (!roomId || !$('inviteView')) return;
  $('inviteRoomTitle').textContent = `Request to join ${formatRoomId(roomId)}`;
  if (state.session) {
    $('inviteConnectionDot').classList.add('on');
    $('inviteSessionName').textContent = state.session.display_name;
    $('inviteSessionId').textContent = state.session.id;
  } else {
    $('inviteConnectionDot').classList.remove('on');
    $('inviteSessionName').textContent = 'Not signed in';
    $('inviteSessionId').textContent = 'Start a guest or local account session first.';
  }

  const existing = outgoingRequestForRoom(roomId);
  $('inviteJoinButton').classList.add('hidden');
  $('inviteRequestButton').classList.remove('hidden');

  if (!state.session) {
    $('inviteStatus').textContent = 'Choose a name, start a session, then send a request to the people already in this room.';
    $('inviteRequestButton').disabled = true;
    $('inviteRequestButton').textContent = 'Sign in to request';
  } else if (state.room?.id === roomId) {
    $('inviteStatus').textContent = 'You are already in this room.';
    $('inviteRequestButton').disabled = true;
    $('inviteRequestButton').textContent = 'Already in room';
  } else if (existing?.status === 'pending') {
    $('inviteStatus').textContent = 'Request sent. Waiting for someone in the room to accept it.';
    $('inviteRequestButton').disabled = true;
    $('inviteRequestButton').textContent = 'Request sent';
  } else if (existing?.status === 'accepted') {
    $('inviteStatus').textContent = 'Your request was accepted. You can join the room now.';
    $('inviteRequestButton').classList.add('hidden');
    $('inviteJoinButton').classList.remove('hidden');
    $('inviteJoinButton').disabled = false;
  } else {
    $('inviteStatus').textContent = `Signed in as ${state.session.display_name}. Send a request when you are ready.`;
    $('inviteRequestButton').disabled = false;
    $('inviteRequestButton').textContent = 'Request to join';
  }
}

async function refreshDirectory() {
  const { entries } = await api('/api/directory', null, 'GET');
  $('directory').innerHTML = '';
  if (!entries.length) {
    const empty = document.createElement('p');
    empty.className = 'hint';
    empty.textContent = 'Nobody is listed yet. Opt in to appear here while online.';
    $('directory').append(empty);
    return;
  }
  for (const entry of entries) {
    const card = document.createElement('div');
    card.className = 'person';
    const name = document.createElement('strong');
    name.textContent = entry.display_name;
    const status = document.createElement('small');
    status.textContent = entry.room_id ? `In ${formatRoomId(entry.room_id)}` : 'Available';
    const actions = document.createElement('div');
    actions.className = 'person-actions';

    if (entry.session_id === state.session?.id) {
      const self = document.createElement('small');
      self.textContent = 'This is you';
      actions.append(self);
    } else if (entry.room_id) {
      const existing = outgoingRequestForRoom(entry.room_id);
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'secondary';
      if (!state.session) {
        button.disabled = true;
        button.textContent = 'Start session to request';
      } else if (state.room?.id === entry.room_id) {
        button.disabled = true;
        button.textContent = 'Already in room';
      } else if (existing?.status === 'pending') {
        button.disabled = true;
        button.textContent = 'Requested';
      } else if (existing?.status === 'accepted') {
        button.textContent = 'Join room';
        button.onclick = () => joinAcceptedRoom(existing);
      } else {
        button.textContent = 'Request to join';
        button.onclick = () => sendJoinRequest(entry.room_id);
      }
      actions.append(button);
    }

    card.append(name, status);
    if (actions.children.length) card.append(actions);
    $('directory').append(card);
  }
}

async function refreshJoinRequests() {
  if (!state.session) {
    state.joinRequests = { incoming: [], outgoing: [] };
    renderJoinRequests();
    return;
  }
  const query = new URLSearchParams({ session_id: state.session.id });
  state.joinRequests = await api(`/api/join-requests?${query}`, null, 'GET');
  renderJoinRequests();
}

function renderJoinRequests() {
  renderIncomingRequests();
  renderOutgoingRequests();
  renderInviteState();
}

function renderIncomingRequests() {
  const list = $('incomingRequests');
  list.innerHTML = '';
  if (!state.session) {
    appendHint(list, 'Start a session to receive room requests.');
    return;
  }
  if (!state.room) {
    appendHint(list, 'Start or join a room to receive requests.');
    return;
  }
  if (!state.joinRequests.incoming.length) {
    appendHint(list, 'No pending requests.');
    return;
  }
  for (const request of state.joinRequests.incoming) {
    const card = requestCard(
      `${request.requester_display_name} wants to join`,
      `${formatRoomId(request.room_id)} · ${request.status}`
    );
    if (request.can_accept) {
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Accept';
      button.onclick = () => acceptJoinRequest(request);
      card.append(button);
    } else {
      appendHint(card, request.room_is_full ? 'Room is full.' : 'Only the host can accept this request.');
    }
    list.append(card);
  }
}

function renderOutgoingRequests() {
  const list = $('outgoingRequests');
  list.innerHTML = '';
  if (!state.session) {
    appendHint(list, 'Start a session to request a room.');
    return;
  }
  if (!state.joinRequests.outgoing.length) {
    appendHint(list, 'No sent requests.');
    return;
  }
  for (const request of state.joinRequests.outgoing) {
    const joined = state.room?.id === request.room_id;
    const card = requestCard(
      formatRoomId(request.room_id),
      joined ? 'Joined' : request.status
    );
    if (request.status === 'accepted' && !joined) {
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Join room';
      button.onclick = () => joinAcceptedRoom(request);
      card.append(button);
    }
    list.append(card);
  }
}

function requestCard(title, meta) {
  const card = document.createElement('div');
  card.className = 'request-card';
  const heading = document.createElement('strong');
  heading.textContent = title;
  const detail = document.createElement('span');
  detail.className = 'request-meta';
  detail.textContent = meta;
  card.append(heading, detail);
  return card;
}

function appendHint(parent, text) {
  const hint = document.createElement('p');
  hint.className = 'hint';
  hint.textContent = text;
  parent.append(hint);
}

async function sendJoinRequest(roomId) {
  try {
    requireSession();
    if (state.room?.id === roomId) {
      setRequestStatus('You are already in that room.');
      renderInviteState();
      return;
    }
    const { request } = await api(`/api/rooms/${roomId}/requests`, { session_id: state.session.id });
    setRequestStatus(request.status === 'accepted' ? 'Your request was already accepted.' : `Request sent to ${formatRoomId(roomId)}.`);
    await refreshJoinRequests();
    await refreshDirectory();
    renderInviteState();
  } catch (error) {
    setRequestStatus(error.message);
    renderInviteState();
  }
}

async function acceptJoinRequest(request) {
  try {
    const { request: accepted } = await api(`/api/join-requests/${request.id}/accept`, { session_id: state.session.id });
    setRequestStatus(`Accepted ${accepted.requester_display_name} for ${formatRoomId(accepted.room_id)}.`);
    await refreshJoinRequests();
    await refreshDirectory();
  } catch (error) {
    setRequestStatus(error.message);
  }
}

async function joinAcceptedRoom(request) {
  try {
    if (state.room && state.room.id !== request.room_id) await leaveCurrentRoom(false, false);
    state.pendingRoomId = null;
    await joinRoom({ id: request.room_id });
    await updateDirectory();
    await refreshJoinRequests();
  } catch (error) {
    setRequestStatus(error.message);
  }
}

async function updateDirectory() {
  if (!state.session) return;
  await api('/api/directory', {
    session_id: state.session.id,
    display_name: state.session.display_name,
    room_id: state.room?.id || null,
    available: $('directoryOptIn').checked,
  });
  await refreshDirectory();
}

async function leaveCurrentRoom(updateListing = true, navigateHome = true) {
  state.roomEpoch += 1;
  send({ type: 'leave' });
  if (state.ws) {
    state.ws.onmessage = null;
    state.ws.onclose = null;
    state.ws.onerror = null;
  }
  state.ws?.close();
  state.ws = null;
  state.peers.forEach(({ pc }) => pc.close());
  state.peers.clear();
  state.participants.clear();
  state.mediaByPeer.clear();
  stopLocalMedia();
  state.room = null;
  $('videos').innerHTML = '';
  updateVideoLayout();
  renderParticipants();
  showLobbyView();
  if (navigateHome && location.pathname !== '/') history.pushState({}, '', '/');
  $('addRandomButton').disabled = true;
  if (updateListing) await updateDirectory();
  state.pendingRoomId = roomIdFromLocation();
}

async function startGuest(displayNameId) {
  setSession((await api('/api/auth/guest', { display_name: $(displayNameId).value || null })).session);
}

async function signup(displayNameId, emailId, passwordId) {
  setSession((await api('/api/auth/signup', { email: $(emailId).value, password: $(passwordId).value, display_name: $(displayNameId).value })).session);
}

async function signin(emailId, passwordId) {
  setSession((await api('/api/auth/signin', { email: $(emailId).value, password: $(passwordId).value })).session);
}

async function runAuth(action, statusId = 'authStatus') {
  try {
    $(statusId).textContent = 'Working...';
    await action();
  } catch (error) {
    $(statusId).textContent = error.message;
    if (statusId !== 'authStatus') $('authStatus').textContent = error.message;
  }
}

async function signout() {
  const sessionId = state.session?.id;
  if (state.room) await leaveCurrentRoom(true, false);
  if (sessionId) await api('/api/auth/signout', { session_id: sessionId }).catch(console.warn);
  clearSession('Signed out.');
  renderInviteState();
}

$('guestButton').onclick = async () => runAuth(() => startGuest('displayName'));
$('signupButton').onclick = async () => runAuth(() => signup('displayName', 'email', 'password'));
$('signinButton').onclick = async () => runAuth(() => signin('email', 'password'));
$('inviteGuestButton').onclick = async () => runAuth(() => startGuest('inviteDisplayName'), 'inviteStatus');
$('inviteSignupButton').onclick = async () => runAuth(() => signup('inviteDisplayName', 'inviteEmail', 'invitePassword'), 'inviteStatus');
$('inviteSigninButton').onclick = async () => runAuth(() => signin('inviteEmail', 'invitePassword'), 'inviteStatus');
$('logoutButton').onclick = signout;
$('inviteLogoutButton').onclick = signout;
$('inviteRequestButton').onclick = async () => {
  const roomId = state.pendingRoomId || roomIdFromLocation();
  if (roomId) await sendJoinRequest(roomId);
};
$('inviteJoinButton').onclick = async () => {
  const roomId = state.pendingRoomId || roomIdFromLocation();
  const request = roomId ? outgoingRequestForRoom(roomId) : null;
  if (request?.status === 'accepted') await joinAcceptedRoom(request);
};
$('startRoomButton').onclick = async () => {
  try {
    requireSession();
    setLaunchStatus('Finding a room...');
    const { room } = await api('/api/rooms/random', {
      session_id: state.session.id,
      size: Number($('roomSize').value || 2),
      host_controls_joiners: $('hostControls').checked,
      share_link_enabled: $('shareLinks').checked,
    });
    await joinRoom(room);
    await updateDirectory();
    await refreshJoinRequests();
    setLaunchStatus('Room connected.');
  } catch (error) {
    setLaunchStatus(error.message);
  }
};
$('addRandomButton').onclick = async () => {
  try {
    setRoomNotice('Calling in another random attendee...');
    const { room } = await api(`/api/rooms/${state.room.id}/add-random`, { session_id: state.session.id, room_id: state.room.id });
    state.room = room;
    renderRoomMeta(room);
    setRoomNotice('The room is open for another random attendee.');
  } catch (error) {
    setRoomNotice(error.message);
  }
};
$('copyLinkButton').onclick = async () => {
  if (!state.room) return;
  const link = `${location.origin}${roomPath(state.room.id)}`;
  try {
    await navigator.clipboard.writeText(link);
    setRoomNotice('Room link copied.');
  } catch {
    setRoomNotice(`Copy failed. Room link: ${link}`);
  }
};
$('leaveButton').onclick = async () => {
  await leaveCurrentRoom();
  await refreshJoinRequests();
};
$('micButton').onclick = toggleMic;
$('cameraButton').onclick = toggleCamera;
$('screenButton').onclick = toggleScreenShare;
$('gridLayoutButton').onclick = () => setLayoutMode('grid');
$('focusLayoutButton').onclick = () => setLayoutMode('focus');
$('tabChat').onclick = () => setActivePanel('chat');
$('tabPeople').onclick = () => setActivePanel('people');
$('tabRequests').onclick = () => setActivePanel('requests');
$('tabSettings').onclick = () => setActivePanel('settings');
$('chatToggleButton').onclick = () => setActivePanel('chat');
$('peopleToggleButton').onclick = () => setActivePanel('people');
$('settingsToggleButton').onclick = () => setActivePanel('settings');
$('cameraSelect').onchange = (event) => changeDevice('video', event.target.value);
$('microphoneSelect').onchange = (event) => changeDevice('audio', event.target.value);
$('mirrorSelfToggle').onchange = (event) => {
  state.media.mirrorSelf = event.target.checked;
  localStorage.setItem('cameraFodderMirrorSelf', String(state.media.mirrorSelf));
  updateVideoCardState('local', currentPresence());
};
$('compactTilesToggle').onchange = (event) => {
  state.media.compactTiles = event.target.checked;
  localStorage.setItem('cameraFodderCompactTiles', String(state.media.compactTiles));
  updateVideoLayout();
};
$('chatForm').onsubmit = (event) => {
  event.preventDefault();
  const text = $('chatInput').value.trim();
  if (text) send({ type: 'chat', text });
  $('chatInput').value = '';
};
$('directoryOptIn').onchange = updateDirectory;
window.addEventListener('beforeunload', () => {
  stopLocalMedia();
  if (state.session) navigator.sendBeacon('/api/directory', new Blob([JSON.stringify({ session_id: state.session.id, display_name: state.session.display_name, room_id: null, available: false })], { type: 'application/json' }));
});
window.addEventListener('popstate', () => {
  const roomId = roomIdFromLocation();
  if (roomId && state.room?.id === roomId) {
    showRoomView(roomId, true);
  } else if (roomId) {
    if (state.room) leaveCurrentRoom(true, false).catch(console.warn);
    showInviteView(roomId, true);
  } else if (!roomId && state.room) {
    leaveCurrentRoom(true, false).catch(console.warn);
  } else if (!roomId) {
    state.pendingRoomId = null;
    showLobbyView();
  }
});

if (state.pendingRoomId) showInviteView(state.pendingRoomId, true);
else showLobbyView();
setRequestStatus();
setLayoutMode(state.layoutMode);
setActivePanel(state.activePanel);
updateControlStates();
renderJoinRequests();
if (state.session) validateStoredSession().catch(console.warn);
refreshDirectory().catch(console.warn);
refreshDeviceLists().catch(console.warn);
setInterval(() => {
  refreshDirectory().catch(console.warn);
  refreshJoinRequests().catch(console.warn);
}, 5000);
