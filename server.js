// 匿名ランダムチャット サーバー (Chat Now)
// Node.js + ws (WebSocket) + Express(静的ファイル配信)

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const crypto = require('crypto');
const cheerio = require('cheerio');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// 待機中ユーザーのキュー
let waitingQueue = [];

// 現在オンラインのユーザー一覧 (id -> client)
const clients = new Map();

// ペア情報 (id -> partnerId)
const pairs = new Map();

// ルーム単位の直近メッセージ(通報時に会話内容を保存するために保持)
const rooms = new Map(); // roomId -> { messages: [{ from: 'a'|'b', text, time }] }
const clientRoom = new Map(); // clientId -> roomId
const MAX_ROOM_MESSAGES = 50;

// 通報一覧(通報された会話だけをここに保存する。全チャットは保存しない)
const reports = [];
const MAX_REPORTS = 500;

// アフィリエイトリンク (画面1のサイドバーに一定間隔でローテーション表示)
const affiliateLinks = []; // { id, text, url }
const AFFILIATE_ROTATE_MS = 15 * 60 * 1000; // 15分

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

    // 新しいルームを作成し、直近メッセージのバッファを用意
    const roomId = crypto.randomUUID();
    rooms.set(roomId, { messages: [] });
    clientRoom.set(a.id, roomId);
    clientRoom.set(b.id, roomId);

    send(a, { type: 'matched' });
    send(b, { type: 'matched' });
  }
}

// ペア解消 + 通知 + ルーム情報のクリーンアップ
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

  const roomId = clientRoom.get(id);
  if (roomId) {
    rooms.delete(roomId);
    clientRoom.delete(id);
    if (partnerId) clientRoom.delete(partnerId);
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

            // 通報時に確認できるよう、ルームの直近メッセージを保持
            const roomId = clientRoom.get(ws.id);
            if (roomId) {
              const room = rooms.get(roomId);
              if (room) {
                room.messages.push({ from: ws.id, text, time: Date.now() });
                if (room.messages.length > MAX_ROOM_MESSAGES) {
                  room.messages.shift();
                }
              }
            }
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
        // 通報受付: そのルームの直近会話をスナップショットして保存
        const partnerId = pairs.get(ws.id);
        const roomId = clientRoom.get(ws.id);
        const room = roomId ? rooms.get(roomId) : null;
        const transcript = room
          ? room.messages.map((m) => ({
              from: m.from === ws.id ? '通報者' : '相手',
              text: m.text,
              time: m.time,
            }))
          : [];

        const report = {
          id: crypto.randomUUID(),
          time: Date.now(),
          reporterId: ws.id,
          targetId: partnerId || null,
          reason: String(msg.reason || '').slice(0, 500),
          transcript,
          status: 'open',
        };

        reports.unshift(report);
        if (reports.length > MAX_REPORTS) {
          reports.length = MAX_REPORTS;
        }

        console.warn(`[REPORT] id=${report.id} from=${ws.id} target=${partnerId || 'unknown'} reason=${report.reason}`);
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

// ===== 管理画面 (Basic認証で保護) =====
// 環境変数 ADMIN_USER / ADMIN_PASS を設定して利用してください。
// 未設定の場合はデフォルト値(admin / changeme)が使われるため、本番では必ず設定すること。
function basicAuth(req, res, next) {
  const user = process.env.ADMIN_USER || 'admin';
  const pass = process.env.ADMIN_PASS || 'changeme';

  const authHeader = req.headers.authorization || '';
  const [scheme, encoded] = authHeader.split(' ');

  if (scheme === 'Basic' && encoded) {
    const decoded = Buffer.from(encoded, 'base64').toString('utf8');
    const sepIndex = decoded.indexOf(':');
    const reqUser = decoded.slice(0, sepIndex);
    const reqPass = decoded.slice(sepIndex + 1);
    if (reqUser === user && reqPass === pass) {
      return next();
    }
  }

  res.set('WWW-Authenticate', 'Basic realm="Chat Now Admin"');
  res.status(401).send('認証が必要です');
}

// ===== 現在の利用状況 =====
// トップページに表示する簡易な利用者数(認証不要・人数のみ)
app.get('/api/stats', (req, res) => {
  res.json({ online: clients.size });
});

// ===== OGP情報取得 =====
// URLからog:image, og:title, og:descriptionを取得（管理画面から呼ばれる）
app.post('/api/fetch-ogp', (req, res) => {
  const url = String((req.body && req.body.url) || '').trim();

  if (!url) {
    return res.status(400).json({ error: 'url is required' });
  }
  if (!/^https?:\/\//i.test(url)) {
    return res.status(400).json({ error: 'url must start with http:// or https://' });
  }

  console.log(`[OGP] Fetching: ${url}`);

  // URLスキームに応じてhttpまたはhttpsを使う
  const isHttps = url.startsWith('https');
  const httpModule = isHttps ? require('https') : require('http');

  function fetchUrl(targetUrl, redirectCount = 0) {
    if (redirectCount > 5) {
      console.log('[OGP] Too many redirects');
      return res.status(400).json({ error: 'Too many redirects' });
    }

    try {
      const targetIsHttps = targetUrl.startsWith('https');
      const targetHttpModule = targetIsHttps ? require('https') : require('http');

      targetHttpModule.get(targetUrl, {
        timeout: 5000,
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
        maxRedirects: 0
      }, (response) => {
        console.log(`[OGP] Status: ${response.statusCode}`);

        // リダイレクト処理
        if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
          console.log(`[OGP] Redirecting to: ${response.headers.location}`);
          return fetchUrl(response.headers.location, redirectCount + 1);
        }

        if (response.statusCode !== 200) {
          console.log(`[OGP] Non-200 status: ${response.statusCode}`);
          return res.status(400).json({ error: `HTTP ${response.statusCode}` });
        }

        let data = '';
        response.on('data', (chunk) => {
          data += chunk;
          if (data.length > 1000000) { // 1MB超えたら中断
            response.destroy();
          }
        });

        response.on('end', () => {
          try {
            console.log(`[OGP] Received ${data.length} bytes`);
            const $ = cheerio.load(data);
            const ogImage = $('meta[property="og:image"]').attr('content') || '';
            const ogTitle = $('meta[property="og:title"]').attr('content') || '';
            const ogDescription = $('meta[property="og:description"]').attr('content') || '';

            console.log(`[OGP] Success - title: "${ogTitle}" image: "${ogImage}"`);
            res.json({
              ok: true,
              ogp: {
                image: ogImage,
                title: ogTitle,
                description: ogDescription,
              },
            });
          } catch (err) {
            console.log(`[OGP] Parse error: ${err.message}`);
            res.status(400).json({ error: 'Failed to parse HTML' });
          }
        });
      }).on('error', (err) => {
        console.log(`[OGP] Fetch error: ${err.message}`);
        res.status(400).json({ error: `Fetch error: ${err.message}` });
      });
    } catch (err) {
      console.log(`[OGP] Exception: ${err.message}`);
      res.status(400).json({ error: `Exception: ${err.message}` });
    }
  }

  fetchUrl(url);
});

// 管理画面向けの詳細な利用状況
app.get('/admin/api/stats', basicAuth, (req, res) => {
  res.json({
    online: clients.size,
    waiting: waitingQueue.length,
    chatting: pairs.size / 2,
  });
});

// 管理画面トップ(通報一覧)
app.get('/admin', basicAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'admin', 'index.html'));
});

// 通報一覧API
app.get('/admin/api/reports', basicAuth, (req, res) => {
  res.json({ reports });
});

// 通報の対応済み/未対応切り替え
app.post('/admin/api/reports/:id/status', basicAuth, (req, res) => {
  const report = reports.find((r) => r.id === req.params.id);
  if (!report) {
    return res.status(404).json({ error: 'not found' });
  }
  const status = req.body && req.body.status;
  if (status !== 'open' && status !== 'resolved') {
    return res.status(400).json({ error: 'invalid status' });
  }
  report.status = status;
  res.json({ ok: true, report });
});

// ===== アフィリエイトリンク =====
// ユーザー画面(画面1サイドバー)に表示する、現在の時間帯に対応するリンクを返す
app.get('/api/affiliate', (req, res) => {
  if (affiliateLinks.length === 0) {
    return res.json({ link: null });
  }
  const index = Math.floor(Date.now() / AFFILIATE_ROTATE_MS) % affiliateLinks.length;
  res.json({ link: affiliateLinks[index] });
});

// 管理画面: アフィリエイトリンク一覧取得
app.get('/admin/api/affiliates', basicAuth, (req, res) => {
  res.json({ links: affiliateLinks });
});

// 管理画面: アフィリエイトリンク追加（OGP対応版）
app.post('/admin/api/affiliates', basicAuth, (req, res) => {
  const title = String((req.body && req.body.title) || '').trim().slice(0, 200);
  const image = String((req.body && req.body.image) || '').trim().slice(0, 500);
  const description = String((req.body && req.body.description) || '').trim().slice(0, 300);
  const url = String((req.body && req.body.url) || '').trim().slice(0, 500);

  if (!url) {
    return res.status(400).json({ error: 'url is required' });
  }
  if (!/^https?:\/\//i.test(url)) {
    return res.status(400).json({ error: 'url must start with http:// or https://' });
  }

  // 後方互換性: titleがない場合は古い形式のtext/urlで対応
  const link = {
    id: crypto.randomUUID(),
    title: title || 'Link',
    image,
    description,
    url,
    // 古い形式用（互換性維持）
    text: title || 'Link',
  };
  affiliateLinks.push(link);
  res.json({ ok: true, link });
});

// 管理画面: アフィリエイトリンク削除
app.delete('/admin/api/affiliates/:id', basicAuth, (req, res) => {
  const idx = affiliateLinks.findIndex((l) => l.id === req.params.id);
  if (idx === -1) {
    return res.status(404).json({ error: 'not found' });
  }
  affiliateLinks.splice(idx, 1);
  res.json({ ok: true });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
