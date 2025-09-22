const admin = require('firebase-admin');

function getServiceAccountFromEnv() {
  // Ambil dari ENV (Railway Variable)
  const b64 = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;
  if (!b64) throw new Error('Missing FIREBASE_SERVICE_ACCOUNT_BASE64');

  // Decode dari base64 → string → parse ke JSON
  const json = Buffer.from(b64, 'base64').toString('utf8');
  return JSON.parse(json);
}

// Inisialisasi admin sekali aja (hindari dobel kalau hot reload)
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(getServiceAccountFromEnv()),
  });
}

// Export messaging biar bisa dipakai di service lain
module.exports = admin.messaging();
