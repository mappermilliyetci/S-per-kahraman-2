// Şimşek Kahraman - oda sunucusu
// Görevi: iki oyuncuyu aynı "oda kodunda" buluşturmak ve aralarındaki mesajları aktarmak.
// Oyunun kendisi (düşmanlar, fizik vb.) davet eden oyuncunun telefonunda çalışır.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_ROOMS = 1000;
const rooms = new Map(); // kod -> { host, guest }

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end('Simsek Kahraman sunucusu calisiyor. Odalar: ' + rooms.size);
});

const wss = new WebSocketServer({ server, maxPayload: 512 * 1024 });

function send(ws, obj) {
  try { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); } catch (e) {}
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.room = null;
  ws.role = null;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    const text = data.toString();
    // Kısa mesajlar kontrol mesajı olabilir; uzun olanlar doğrudan karşı tarafa aktarılır
    if (text.length < 80 && text.indexOf('"_') !== -1) {
      let m;
      try { m = JSON.parse(text); } catch (e) { return; }
      handleControl(ws, m);
      return;
    }
    const r = ws.room && rooms.get(ws.room);
    if (!r) return;
    const other = ws.role === 'host' ? r.guest : r.host;
    if (other && other.readyState === 1) {
      // Arkadaş tarafı yavaşsa görüntü paketlerini at (gecikme birikmesin)
      if (other.bufferedAmount > 400000 && text.indexOf('"t":"s"') !== -1) return;
      other.send(text);
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

function handleControl(ws, m) {
  const t = m.t;
  if (t === '_ping') return;
  const code = String(m.code || '').toUpperCase();
  if (t === '_create') {
    if (!/^[A-Z0-9]{3,8}$/.test(code)) return send(ws, { t: '_err', c: 'bad-code' });
    if (ws.room) leave(ws);
    if (rooms.has(code)) return send(ws, { t: '_err', c: 'id-taken' });
    if (rooms.size >= MAX_ROOMS) return send(ws, { t: '_err', c: 'busy' });
    rooms.set(code, { host: ws, guest: null });
    ws.room = code; ws.role = 'host';
    return send(ws, { t: '_created' });
  }
  if (t === '_join') {
    const r = rooms.get(code);
    if (!r || !r.host || r.host.readyState !== 1) return send(ws, { t: '_err', c: 'peer-unavailable' });
    if (r.guest && r.guest.readyState === 1) return send(ws, { t: '_err', c: 'full' });
    if (ws.room) leave(ws);
    r.guest = ws; ws.room = code; ws.role = 'guest';
    send(ws, { t: '_joined' });
    send(r.host, { t: '_peer' });
  }
}

function leave(ws) {
  const code = ws.room;
  if (!code) return;
  ws.room = null;
  const r = rooms.get(code);
  if (!r) return;
  if (ws.role === 'host') {
    // Oda sahibi çıkınca oda kapanır
    if (r.guest) { send(r.guest, { t: '_left' }); try { r.guest.close(); } catch (e) {} }
    rooms.delete(code);
  } else if (r.guest === ws) {
    r.guest = null;
    send(r.host, { t: '_left' });
  }
}

// Kopan bağlantıları temizle
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { try { ws.terminate(); } catch (e) {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 30000);

server.listen(PORT, () => console.log('Sunucu hazir, port ' + PORT));
