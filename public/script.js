const statusBadge = document.getElementById('statusBadge');
const onlineCountEl = document.getElementById('onlineCount');
const affiliateBox = document.getElementById('affiliateBox');
const affLink = document.getElementById('affLink');
const startScreen = document.getElementById('startScreen');
const chatScreen = document.getElementById('chatScreen');
const chatStatus = document.getElementById('chatStatus');
const messagesEl = document.getElementById('messages');
const messageForm = document.getElementById('messageForm');
const messageInput = document.getElementById('messageInput');
const sendBtn = document.getElementById('sendBtn');
const skipBtn = document.getElementById('skipBtn');
const leaveBtn = document.getElementById('leaveBtn');
const reportBtn = document.getElementById('reportBtn');
const startBtn = document.getElementById('startBtn');
const agreeCheck = document.getElementById('agreeCheck');
const typingIndicator = document.getElementById('typingIndicator');
const rulesModal = document.getElementById('rulesModal');

let ws = null;
let typingTimeout = null;

// 利用ルール同意でスタートボタン有効化
agreeCheck.addEventListener('change', () => {
  startBtn.disabled = !agreeCheck.checked;
});

[document.getElementById('rulesLink'), document.getElementById('rulesLink2')].forEach((el) => {
  el.addEventListener('click', (e) => {
    e.preventDefault();
    rulesModal.classList.remove('hidden');
  });
});

document.getElementById('closeRules').addEventListener('click', () => {
  rulesModal.classList.add('hidden');
});

function addMessage(text, type) {
  const div = document.createElement('div');
  div.className = `msg ${type}`;
  div.textContent = text;
  messagesEl.appendChild(div);
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function setChatState(state) {
  // state: 'waiting' | 'chatting' | 'ended'
  if (state === 'waiting') {
    chatStatus.textContent = 'マッチング中...';
    messageInput.disabled = true;
    sendBtn.disabled = true;
  } else if (state === 'chatting') {
    chatStatus.textContent = '相手と接続しました';
    messageInput.disabled = false;
    sendBtn.disabled = false;
    messageInput.focus();
  } else if (state === 'ended') {
    chatStatus.textContent = '会話は終了しました';
    messageInput.disabled = true;
    sendBtn.disabled = true;
  }
}

function connectWebSocket() {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${protocol}//${location.host}`);

  ws.addEventListener('open', () => {
    statusBadge.textContent = '接続済み';
  });

  ws.addEventListener('close', () => {
    statusBadge.textContent = '切断されました';
    setChatState('ended');
  });

  ws.addEventListener('message', (event) => {
    const msg = JSON.parse(event.data);

    switch (msg.type) {
      case 'connected':
        statusBadge.textContent = '接続済み';
        break;

      case 'waiting':
        messagesEl.innerHTML = '';
        setChatState('waiting');
        addMessage('相手を探しています...', 'system');
        break;

      case 'matched':
        setChatState('chatting');
        addMessage('相手が見つかりました。チャットを始めましょう!', 'system');
        break;

      case 'message':
        addMessage(msg.text, 'them');
        hideTyping();
        break;

      case 'typing':
        showTyping();
        break;

      case 'partner_left':
        addMessage('相手が退出しました。「スキップ」で次の相手を探せます。', 'system');
        setChatState('ended');
        break;

      case 'left':
        setChatState('ended');
        break;

      case 'report_received':
        addMessage('通報を受け付けました。ご協力ありがとうございます。', 'system');
        break;

      default:
        break;
    }
  });
}

function showTyping() {
  typingIndicator.classList.remove('hidden');
  clearTimeout(typingTimeout);
  typingTimeout = setTimeout(hideTyping, 1500);
}

function hideTyping() {
  typingIndicator.classList.add('hidden');
}

startBtn.addEventListener('click', () => {
  startScreen.classList.add('hidden');
  chatScreen.classList.remove('hidden');

  if (!ws || ws.readyState !== WebSocket.OPEN) {
    connectWebSocket();
    ws.addEventListener('open', () => {
      ws.send(JSON.stringify({ type: 'find' }));
    }, { once: true });
  } else {
    ws.send(JSON.stringify({ type: 'find' }));
  }
});

messageForm.addEventListener('submit', (e) => {
  e.preventDefault();
  const text = messageInput.value.trim();
  if (!text || !ws) return;
  ws.send(JSON.stringify({ type: 'message', text }));
  addMessage(text, 'me');
  messageInput.value = '';
});

let lastTypingSent = 0;
messageInput.addEventListener('input', () => {
  const now = Date.now();
  if (now - lastTypingSent > 800 && ws) {
    ws.send(JSON.stringify({ type: 'typing' }));
    lastTypingSent = now;
  }
});

skipBtn.addEventListener('click', () => {
  if (!ws) return;
  messagesEl.innerHTML = '';
  ws.send(JSON.stringify({ type: 'skip' }));
  setChatState('waiting');
});

leaveBtn.addEventListener('click', () => {
  if (ws) {
    ws.send(JSON.stringify({ type: 'leave' }));
  }
  setChatState('ended');
  chatScreen.classList.add('hidden');
  startScreen.classList.remove('hidden');
});

reportBtn.addEventListener('click', () => {
  if (!ws) return;
  const reason = prompt('通報理由を入力してください(任意):') || '';
  ws.send(JSON.stringify({ type: 'report', reason }));
});

// 現在の利用者数(オンライン人数)を定期的に取得して表示
async function updateOnlineCount() {
  try {
    const res = await fetch('/api/stats');
    if (!res.ok) throw new Error('failed');
    const data = await res.json();
    onlineCountEl.textContent = `オンライン: ${data.online}人`;
  } catch (e) {
    onlineCountEl.textContent = 'オンライン: -人';
  }
}

updateOnlineCount();
setInterval(updateOnlineCount, 5000);

// アフィリエイトリンク(サイドバー「おすすめ」欄)を取得して表示
// サーバー側で15分ごとに表示するリンクが切り替わる
async function updateAffiliate() {
  try {
    const res = await fetch('/api/affiliate');
    if (!res.ok) throw new Error('failed');
    const data = await res.json();
    if (data.link) {
      affiliateBox.classList.remove('hidden');
      affLink.textContent = `${data.link.text} →`;
      affLink.href = data.link.url;
    } else {
      affiliateBox.classList.add('hidden');
    }
  } catch (e) {
    // 取得失敗時は何もしない
  }
}

updateAffiliate();
setInterval(updateAffiliate, 60 * 1000);
