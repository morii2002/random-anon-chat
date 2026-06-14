// 匿名ランダムチャット サーバー
// Node.js + ws (WebSocket) + Express(静的ファイル配信)

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const crypto = require('crypto');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// 待機中ユーザーのキュー
let waitingQueue = [];

// 現在オンラインのユーザー一覧 (id -> client)
const clients = new Map();

// ペア情報 (id -> partnerId)
const pairs = new Map();

function send(client, data) {
  if (client && client.readyState === client.OPEN) {
    client.send(JSON.stringify(data));
  }
}

function removeFromQueue(id) {
  waitingQueue = waitingQueue.filter((c) => c.id !== id);
}

// マッチング処理: 待機キューから2人組ませる
function tryMatch() {
  while (waitingQueue.length >= 2) {
    const a = waitingQueue.shift();
    const b = waitingQueue.shift();

    // 接続が切れている場合はスキップ
    if (a.readyState !== a.OPEN) continue;
    if (b.readyState !== b.OPEN) {
      waitingQueue.unshift(a);
      continue;
    }

    pairs.set(a.id, b.id);
    pairs.set(b.id, a.id);

    send(a, { type: 'matched' });
    send(b, { type: 'matched' });
  }
}

// ペア解消 + 通知
function disconnectPair(id, notifyPartner = true) {
  const partnerId = pairs.get(id);
  if (partnerId) {
    pairs.delete(id);
    pairs.delete(partnerId);
    if (notifyPartner) {
      const partner = clients.get(partnerId);
      send(partner, { type: 'partner_left' });
    }
  }
}

wss.on('connection', (ws) => {
  ws.id = crypto.randomUUID();
  ws.isAlive = true;
  clients.set(ws.id, ws);

  send(ws, { type: 'connected', id: ws.id });

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {
      case 'find': {
        // 既存ペアがあれば解消してからキューへ
        disconnectPair(ws.id);
        removeFromQueue(ws.id);
        waitingQueue.push(ws);
        send(ws, { type: 'waiting' });
        tryMatch();
        break;
      }

      case 'message': {
        const partnerId = pairs.get(ws.id);
        if (partnerId) {
          const partner = clients.get(partnerId);
          // 文字数制限・簡易サニタイズ
          const text = String(msg.text || '').slice(0, 1000);
          if (text.trim().length > 0) {
            send(partner, { type: 'message', text });
          }
        }
        break;
      }

      case 'typing': {
        const partnerId = pairs.get(ws.id);
        if (partnerId) {
          send(clients.get(partnerId), { type: 'typing' });
        }
        break;
      }

      case 'skip': {
        // 自分から切断し、再度キューに入る
        disconnectPair(ws.id);
        removeFromQueue(ws.id);
        waitingQueue.push(ws);
        send(ws, { type: 'waiting' });
        tryMatch();
        break;
      }

      case 'leave': {
        disconnectPair(ws.id);
        removeFromQueue(ws.id);
        send(ws, { type: 'left' });
        break;
      }

      case 'report': {
        // 通報受付(ログのみ。実運用では永続化・管理画面連携を推奨)
        const partnerId = pairs.get(ws.id);
        console.warn(`[REPORT] from=${ws.id} target=${partnerId || 'unknown'} reason=${msg.reason || ''}`);
        send(ws, { type: 'report_received' });
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    disconnectPair(ws.id);
    removeFromQueue(ws.id);
    clients.delete(ws.id);
  });

  ws.on('error', () => {
    disconnectPair(ws.id);
    removeFromQueue(ws.id);
    clients.delete(ws.id);
  });
});

// 接続確認用ping (死活監視)
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => clearInterval(interval));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
