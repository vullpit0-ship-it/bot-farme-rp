const {
  Client,
  GatewayIntentBits,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  EmbedBuilder,
  Partials,
} = require("discord.js");

console.log("🔥 BUILD FINAL (DB UNIQUE + BUTTONS FIX) 🔥", new Date().toISOString());

const express = require("express");
const { Pool } = require("pg");

// ==========================
// ✅ CONFIG POR SERVIDOR (2 servidores)
// ==========================
const GUILD_ID_TESTE = "1477774289414656213";
const GUILD_ID_NOVA_ORDEM = "1469111028796227728";

const CONFIGS = {
  [GUILD_ID_TESTE]: {
    NAME: "TESTE",
    GERENTE_ROLE_ID: "1477779548484538539",
    ROLE_00_ID: "1477850489189044365",
    LOG_CHANNEL_ID: "1477800551340310651",
    ENVIO_FARME_CHANNEL_ID: "1477777883714818098",
  },

  [GUILD_ID_NOVA_ORDEM]: {
    NAME: "NOVA ORDEM",
    GERENTE_ROLE_ID: "1469111029161136386",
    ROLE_00_ID: "1469111029161136392",
    LOG_CHANNEL_ID: "1478096038114885704",
    ENVIO_FARME_CHANNEL_ID: "1478095912789082284",
  },
};

function getCfg(guildId) {
  return CONFIGS[guildId] || null;
}

// ✅ Opcional: logo custom pro log (Render -> Environment -> LOGO_URL)
const LOGO_URL = process.env.LOGO_URL || null;

// ✅ Meta diária obrigatória
const META_DIARIA = 100;

// ✅ Horário do “fechamento do dia”
const DAILY_AUDIT_HOUR = 0;
const DAILY_AUDIT_MIN = 5;

// ==========================
// 🌐 WEB (mantém acordado com UptimeRobot)
// ==========================
const app = express();
app.get("/", (req, res) => res.send("Bot online ✅"));
app.listen(process.env.PORT || 3000, () => console.log("Web OK"));

// ==========================
// 🗄️ POSTGRES (SUPABASE)
// ==========================
if (!process.env.DATABASE_URL) {
  console.error("Faltando DATABASE_URL nas env vars (Render).");
  process.exit(1);
}
if (!process.env.DISCORD_TOKEN) {
  console.error("Faltando DISCORD_TOKEN nas env vars (Render).");
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

function nowBR() {
  return new Date().toLocaleString("pt-BR");
}
function todayKey() {
  return new Date().toDateString();
}
function yesterdayKey() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toDateString();
}

// thumbnail do embed (logo custom OU avatar do bot)
function getThumb(interactionOrClient) {
  const avatar =
    interactionOrClient?.user?.displayAvatarURL?.() ||
    interactionOrClient?.client?.user?.displayAvatarURL?.() ||
    null;
  return LOGO_URL || avatar || null;
}

// ==========================
// 🧱 INIT DB (VERSÃO FINAL)
// ==========================
async function initDB() {
  // Cria tabelas se não existirem
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      "guildId" TEXT NOT NULL,
      "userId"  TEXT NOT NULL,
      "ultimoDia" TEXT,
      "papelHoje" INTEGER DEFAULT 0,
      "sementesHoje" INTEGER DEFAULT 0,
      "papelCarry" INTEGER DEFAULT 0,
      "sementesCarry" INTEGER DEFAULT 0,
      "papelDebt" INTEGER DEFAULT 0,
      "sementesDebt" INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS historico (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT,
      "userId" TEXT,
      tipo TEXT,
      quantidade INTEGER,
      status TEXT,
      data TEXT,
      dia TEXT,
      "msgId" TEXT,
      "gerenteId" TEXT,
      aplicado INTEGER,
      carry INTEGER,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS bot_config (
      chave TEXT PRIMARY KEY,
      valor TEXT
    );
  `);

  // Garante UNIQUE para o ON CONFLICT funcionar (SEM mexer em PK antiga)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS usuarios_guild_user_unique
    ON usuarios ("guildId","userId");
  `);
  console.log("DB: UNIQUE usuarios_guild_user_unique OK");

  // Índices úteis
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_msgId ON historico ("msgId");`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_dia ON historico ("dia");`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_created_at ON historico (created_at);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_guild_user_dia ON historico ("guildId","userId","dia");`);

  // Corrige valores null antigos
  await pool.query(
    `
    UPDATE usuarios
    SET
      "papelHoje" = COALESCE("papelHoje", 0),
      "sementesHoje" = COALESCE("sementesHoje", 0),
      "papelCarry" = COALESCE("papelCarry", 0),
      "sementesCarry" = COALESCE("sementesCarry", 0),
      "papelDebt" = COALESCE("papelDebt", 0),
      "sementesDebt" = COALESCE("sementesDebt", 0),
      "ultimoDia" = COALESCE("ultimoDia", $1)
  `,
    [todayKey()]
  );

  console.log("DB OK (PostgreSQL / Supabase)");
}

// ==========================
// 🤖 CLIENT
// ==========================
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers, // ✅ roles/cache
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Channel, Partials.Message, Partials.GuildMember],
});

client.once("clientReady", () => {
  console.log("Bot online como", client.user?.tag);
  startDailyAuditLoop().catch((e) => console.error("Erro startDailyAuditLoop:", e));
});

// ==========================
// 🧠 HELPERS
// ==========================
function getLogChannel(guild) {
  const cfg = getCfg(guild.id);
  if (!cfg) return null;
  return guild.channels.cache.get(cfg.LOG_CHANNEL_ID) || null;
}

function isEnvioChannel(message) {
  const cfg = getCfg(message.guild.id);
  if (!cfg) return false;
  return message.channel.id === cfg.ENVIO_FARME_CHANNEL_ID;
}

async function ensureUser(guildId, userId) {
  let res = await pool.query(`SELECT * FROM usuarios WHERE "guildId"=$1 AND "userId"=$2`, [guildId, userId]);
  if (res.rows[0]) return res.rows[0];

  const hoje = todayKey();

  // ✅ ON CONFLICT agora funciona por causa do UNIQUE INDEX
  await pool.query(
    `INSERT INTO usuarios ("guildId","userId","ultimoDia","papelHoje","sementesHoje","papelCarry","sementesCarry","papelDebt","sementesDebt")
     VALUES ($1,$2,$3,0,0,0,0,0,0)
     ON CONFLICT ("guildId","userId") DO NOTHING`,
    [guildId, userId, hoje]
  );

  res = await pool.query(`SELECT * FROM usuarios WHERE "guildId"=$1 AND "userId"=$2`, [guildId, userId]);
  return res.rows[0];
}

// quando vira o dia: começa o dia com o extra
async function rollToToday(guildId, userId) {
  const hoje = todayKey();
  const u = await ensureUser(guildId, userId);

  if (u.ultimoDia === hoje) return u;

  const papelCarry = Number(u.papelCarry || 0);
  const sementesCarry = Number(u.sementesCarry || 0);

  const papelHoje = Math.min(100, papelCarry);
  const sementesHoje = Math.min(100, sementesCarry);

  const novoPapelCarry = Math.max(0, papelCarry - 100);
  const novoSementesCarry = Math.max(0, sementesCarry - 100);

  await pool.query(
    `UPDATE usuarios
     SET "ultimoDia"=$1,
         "papelHoje"=$2,
         "sementesHoje"=$3,
         "papelCarry"=$4,
         "sementesCarry"=$5
     WHERE "guildId"=$6 AND "userId"=$7`,
    [hoje, papelHoje, sementesHoje, novoPapelCarry, novoSementesCarry, guildId, userId]
  );

  const res = await pool.query(`SELECT * FROM usuarios WHERE "guildId"=$1 AND "userId"=$2`, [guildId, userId]);
  return res.rows[0];
}

async function insertHistorico({ guildId, userId, tipo, quantidade, status, msgId, gerenteId, aplicado, carry, dia }) {
  await pool.query(
    `INSERT INTO historico ("guildId","userId",tipo,quantidade,status,data,dia,"msgId","gerenteId",aplicado,carry)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
    [guildId, userId, tipo, quantidade, status, nowBR(), dia || todayKey(), msgId, gerenteId, aplicado ?? null, carry ?? null]
  );
}

async function alreadyProcessed(guildId, msgId) {
  const { rows } = await pool.query(
    `SELECT id FROM historico WHERE "guildId"=$1 AND "msgId"=$2 AND (status='APROVADO' OR status='NEGADO') LIMIT 1`,
    [guildId, msgId]
  );
  return !!rows[0];
}

function formatDebtLine(papelDebt, sementesDebt) {
  const parts = [];
  if (papelDebt > 0) parts.push(`PAPEL **${papelDebt}**`);
  if (sementesDebt > 0) parts.push(`SEMENTES **${sementesDebt}**`);
  if (!parts.length) return null;
  return `❌ Farme atrasado: ${parts.join(" | ")}`;
}

function formatFaltamLine(faltamP, faltamS) {
  return `📌 Hoje faltam pra meta: PAPEL **${faltamP}** | SEMENTES **${faltamS}**`;
}

function formatAtrasadoTotalLine(papelDebt, sementesDebt) {
  const total = Math.max(0, Number(papelDebt || 0)) + Math.max(0, Number(sementesDebt || 0));
  return `📦 Atrasado total acumulado: **${total}**`;
}

// ✅ total aprovado do dia (aplicado)
async function getTotaisDia(guildId, userId, diaStr) {
  const { rows } = await pool.query(
    `
    SELECT
      COALESCE(SUM(CASE WHEN status='APROVADO' AND tipo='papel' THEN COALESCE(aplicado,0) ELSE 0 END),0)::int AS papel_total,
      COALESCE(SUM(CASE WHEN status='APROVADO' AND tipo='sementes' THEN COALESCE(aplicado,0) ELSE 0 END),0)::int AS sementes_total
    FROM historico
    WHERE "guildId"=$1 AND "userId"=$2 AND dia=$3
  `,
    [guildId, userId, diaStr]
  );

  return {
    papel: rows[0]?.papel_total ?? 0,
    sementes: rows[0]?.sementes_total ?? 0,
  };
}

// aplica aprovação respeitando limite 100/dia por tipo
// overflow (>100) quita dívida; o que sobrar vira extra
async function applyFarm(guildId, userId, tipo, quantidade) {
  const u0 = await rollToToday(guildId, userId);

  let papelHoje = Number(u0.papelHoje || 0);
  let sementesHoje = Number(u0.sementesHoje || 0);
  let papelCarry = Number(u0.papelCarry || 0);
  let sementesCarry = Number(u0.sementesCarry || 0);
  let papelDebt = Number(u0.papelDebt || 0);
  let sementesDebt = Number(u0.sementesDebt || 0);

  let aplicado = 0;
  let carry = 0;
  let quitouDebt = 0;

  if (tipo === "papel") {
    const restante = Math.max(0, 100 - papelHoje);
    aplicado = Math.min(quantidade, restante);

    let overflow = Math.max(0, quantidade - aplicado);

    const paga = Math.min(overflow, papelDebt);
    papelDebt -= paga;
    overflow -= paga;
    quitouDebt = paga;

    carry = overflow;

    papelHoje += aplicado;
    papelCarry += carry;

    await pool.query(
      `UPDATE usuarios SET "papelHoje"=$1,"papelCarry"=$2,"papelDebt"=$3 WHERE "guildId"=$4 AND "userId"=$5`,
      [papelHoje, papelCarry, papelDebt, guildId, userId]
    );
  } else if (tipo === "sementes") {
    const restante = Math.max(0, 100 - sementesHoje);
    aplicado = Math.min(quantidade, restante);

    let overflow = Math.max(0, quantidade - aplicado);

    const paga = Math.min(overflow, sementesDebt);
    sementesDebt -= paga;
    overflow -= paga;
    quitouDebt = paga;

    carry = overflow;

    sementesHoje += aplicado;
    sementesCarry += carry;

    await pool.query(
      `UPDATE usuarios SET "sementesHoje"=$1,"sementesCarry"=$2,"sementesDebt"=$3 WHERE "guildId"=$4 AND "userId"=$5`,
      [sementesHoje, sementesCarry, sementesDebt, guildId, userId]
    );
  } else {
    throw new Error("tipo inválido");
  }

  const u1 = await pool.query(`SELECT * FROM usuarios WHERE "guildId"=$1 AND "userId"=$2`, [guildId, userId]);
  return { user: u1.rows[0], aplicado, carry, quitouDebt };
}

// ==========================
// ✅ FECHAMENTO DIÁRIO
// ==========================
async function getConfig(chave, defaultValue = null) {
  const { rows } = await pool.query(`SELECT valor FROM bot_config WHERE chave=$1`, [chave]);
  return rows[0]?.valor ?? defaultValue;
}
async function setConfig(chave, valor) {
  await pool.query(
    `INSERT INTO bot_config (chave, valor) VALUES ($1,$2)
     ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor`,
    [chave, String(valor)]
  );
}

function chunkLines(lines, maxLen = 3500) {
  const chunks = [];
  let cur = "";
  for (const line of lines) {
    if ((cur + "\n" + line).length > maxLen) {
      chunks.push(cur);
      cur = line;
    } else {
      cur = cur ? cur + "\n" + line : line;
    }
  }
  if (cur) chunks.push(cur);
  return chunks;
}

async function runDailyAuditOnce() {
  const now = new Date();
  const hh = now.getHours();
  const mm = now.getMinutes();
  if (!(hh === DAILY_AUDIT_HOUR && mm === DAILY_AUDIT_MIN)) return;

  const today = todayKey();
  const diaAuditado = yesterdayKey();

  for (const guild of client.guilds.cache.values()) {
    const cfg = getCfg(guild.id);
    if (!cfg) continue;

    const lastDoneKey = `${guild.id}:last_daily_audit_day`;
    const lastDone = await getConfig(lastDoneKey, "");
    if (lastDone === today) continue;

    const { rows: users } = await pool.query(`SELECT "userId" FROM usuarios WHERE "guildId"=$1`, [guild.id]);
    const faltaram = [];

    for (const u of users) {
      const userId = u.userId;
      const totals = await getTotaisDia(guild.id, userId, diaAuditado);

      const faltouP = Math.max(0, META_DIARIA - totals.papel);
      const faltouS = Math.max(0, META_DIARIA - totals.sementes);

      if (faltouP > 0 || faltouS > 0) {
        await pool.query(
          `UPDATE usuarios
           SET "papelDebt" = COALESCE("papelDebt",0) + $1,
               "sementesDebt" = COALESCE("sementesDebt",0) + $2
           WHERE "guildId"=$3 AND "userId"=$4`,
          [faltouP, faltouS, guild.id, userId]
        );

        faltaram.push({
          userId,
          papel_total: totals.papel,
          sementes_total: totals.sementes,
          faltouP,
          faltouS,
        });
      }
    }

    const logChannel = getLogChannel(guild);
    if (!logChannel) continue;

    const thumb = getThumb(client);

    if (!faltaram.length) {
      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle(`✅ Fechamento diário (META) — ${cfg.NAME}`)
        .setDescription(`📅 Dia auditado: **${diaAuditado}**\n\n✅ Ninguém ficou abaixo da meta (${META_DIARIA}).`)
        .setTimestamp();
      if (thumb) embed.setThumbnail(thumb);
      await logChannel.send({ embeds: [embed] }).catch(() => null);
    } else {
      const lines = faltaram.map((r) => {
        const pTxt = r.faltouP > 0 ? `❌ ${r.papel_total}/${META_DIARIA} (faltou ${r.faltouP})` : `✅ ${r.papel_total}/${META_DIARIA}`;
        const sTxt = r.faltouS > 0 ? `❌ ${r.sementes_total}/${META_DIARIA} (faltou ${r.faltouS})` : `✅ ${r.sementes_total}/${META_DIARIA}`;
        return `• <@${r.userId}> — 📄 ${pTxt} | 🌱 ${sTxt}`;
      });

      const chunks = chunkLines(lines);

      for (let i = 0; i < chunks.length; i++) {
        const embed = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle(`⚠️ Fechamento diário: NÃO bateu meta — ${cfg.NAME}`)
          .setDescription(
            `📅 Dia auditado: **${diaAuditado}**\n🎯 Meta diária: **${META_DIARIA}**\n📌 A diferença foi somada no **Farme atrasado**.\n\n${chunks[i]}`
          )
          .setFooter({
            text: i === 0 ? "Meta obrigatória: abaixo da meta vira dívida acumulada." : `Continuação (${i + 1}/${chunks.length})`,
          })
          .setTimestamp();
        if (thumb) embed.setThumbnail(thumb);
        await logChannel.send({ embeds: [embed] }).catch(() => null);
      }
    }

    await setConfig(lastDoneKey, today);
  }
}

async function startDailyAuditLoop() {
  setInterval(() => {
    runDailyAuditOnce().catch((e) => console.error("Erro runDailyAuditOnce:", e));
  }, 30_000);
}

// ==========================
// 📩 MESSAGE
// ==========================
client.on("messageCreate", async (message) => {
  try {
    if (!message.guild || message.author.bot) return;

    const cfg = getCfg(message.guild.id);
    if (!cfg) return;

    console.log("[MSG]", message.guild.id, message.channel.id, message.author.tag, JSON.stringify(message.content));

    const member = message.member || (await message.guild.members.fetch(message.author.id));
    const isGerente = member.roles.cache.has(cfg.GERENTE_ROLE_ID);
    const is00 = member.roles.cache.has(cfg.ROLE_00_ID);

    const content = (message.content || "").trim();
    const lower = content.toLowerCase();

    // ✅ STATUS
    if (lower === "!status") {
      const u = await rollToToday(message.guild.id, message.author.id);

      const totalsHoje = await getTotaisDia(message.guild.id, message.author.id, todayKey());
      const faltamP = Math.max(0, META_DIARIA - (totalsHoje.papel || 0));
      const faltamS = Math.max(0, META_DIARIA - (totalsHoje.sementes || 0));

      const papelDebt = Number(u.papelDebt || 0);
      const sementesDebt = Number(u.sementesDebt || 0);
      const debtLine = formatDebtLine(papelDebt, sementesDebt);

      const txt =
        `🏷️ Servidor: **${cfg.NAME}**\n` +
        `👤 ${message.author}\n\n` +
        `📄 **Papel:** ${u.papelHoje}/100 (extra: ${u.papelCarry})\n` +
        `🌱 **Sementes:** ${u.sementesHoje}/100 (extra: ${u.sementesCarry})\n\n` +
        `${formatFaltamLine(faltamP, faltamS)}\n` +
        `${formatAtrasadoTotalLine(papelDebt, sementesDebt)}\n` +
        (debtLine ? `${debtLine}\n` : "✅ Farme atrasado: 0\n") +
        `\n🕒 Dia: **${u.ultimoDia}**`;

      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle("📌 Seu Status")
        .setDescription(txt)
        .setFooter({ text: "Comando: !status" })
        .setTimestamp();

      const thumb = getThumb(client);
      if (thumb) embed.setThumbnail(thumb);

      return message.reply({ embeds: [embed] });
    }

    // 🔥 FARME
    if (lower.startsWith("!farme")) {
      if (!isEnvioChannel(message)) return;

      const args = content.split(/\s+/);
      const tipo = (args[1] || "").toLowerCase();
      const quantidade = parseInt(args[2], 10);

      if (!["papel", "sementes"].includes(tipo)) {
        return message.reply("❌ Tipo inválido. Use: `papel` ou `sementes`.\nEx: `!farme papel 10`");
      }
      if (isNaN(quantidade) || quantidade <= 0) {
        return message.reply("❌ Quantidade inválida. Ex: `!farme sementes 25`");
      }
      if (!message.attachments.size) {
        return message.reply("❌ Envie o print junto.");
      }

      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`aprovar_${message.author.id}_${quantidade}_${tipo}`)
          .setLabel("Aprovar")
          .setStyle(ButtonStyle.Success),
        new ButtonBuilder()
          .setCustomId(`negar_${message.author.id}_${quantidade}_${tipo}`)
          .setLabel("Negar")
          .setStyle(ButtonStyle.Danger)
      );

      return message.reply({
        content:
          `📥 Farme enviado por ${message.author}\n` +
          `📦 **${tipo.toUpperCase()}** • **${quantidade}**\n` +
          `⏳ Aguardando gerente/00...`,
        components: [row],
      });
    }

    // ✏️ EDITAR (SÓ 00) - inclui extra
    if (lower.startsWith("!editar")) {
      if (!is00) return message.reply("❌ Apenas cargo **00** pode usar.");

      const user = message.mentions.users.first();
      const parts = content.split(/\s+/);
      const tipo = (parts[2] || "").toLowerCase();
      const valor = parseInt(parts[3], 10);

      if (!user || isNaN(valor)) {
        return message.reply(
          "Use:\n" +
            "`!editar @usuario papel +50`\n" +
            "`!editar @usuario sementes -10`\n" +
            "`!editar @usuario extra_papel +30`\n" +
            "`!editar @usuario extra_sementes -20`"
        );
      }
      if (user.id === message.author.id) {
        return message.reply("❌ Você não pode editar o próprio farme.");
      }

      const logChannel = getLogChannel(message.guild);
      const before = await rollToToday(message.guild.id, user.id);

      const sendEditLog = async (titulo, desc) => {
        if (!logChannel) return;
        const embed = new EmbedBuilder().setColor("#2b2d31").setTitle(titulo).setDescription(desc).setTimestamp();
        const thumb = getThumb(client);
        if (thumb) embed.setThumbnail(thumb);
        logChannel.send({ embeds: [embed] }).catch(() => null);
      };

      if (tipo === "papel") {
        const antes = Number(before.papelHoje || 0);
        const depois = Math.max(0, antes + valor);

        await pool.query(`UPDATE usuarios SET "papelHoje"=$1 WHERE "guildId"=$2 AND "userId"=$3`, [
          depois,
          message.guild.id,
          user.id,
        ]);

        await insertHistorico({
          guildId: message.guild.id,
          userId: user.id,
          tipo: "papel",
          quantidade: valor,
          status: "AJUSTE",
          msgId: "manual",
          gerenteId: message.author.id,
          aplicado: valor,
          carry: 0,
          dia: todayKey(),
        });

        await sendEditLog(
          `🛠️ AJUSTE MANUAL (00) — ${cfg.NAME}`,
          `👤 Membro: <@${user.id}>\n🧾 Tipo: **PAPEL**\n✏️ Ajuste: **${valor >= 0 ? "+" : ""}${valor}**\n📌 Antes: **${antes}** → Depois: **${depois}**\n🛡️ Feito por: <@${message.author.id}>`
        );

        return message.reply(`✅ Papel atualizado: <@${user.id}> **${depois}/100**`);
      }

      if (tipo === "sementes") {
        const antes = Number(before.sementesHoje || 0);
        const depois = Math.max(0, antes + valor);

        await pool.query(`UPDATE usuarios SET "sementesHoje"=$1 WHERE "guildId"=$2 AND "userId"=$3`, [
          depois,
          message.guild.id,
          user.id,
        ]);

        await insertHistorico({
          guildId: message.guild.id,
          userId: user.id,
          tipo: "sementes",
          quantidade: valor,
          status: "AJUSTE",
          msgId: "manual",
          gerenteId: message.author.id,
          aplicado: valor,
          carry: 0,
          dia: todayKey(),
        });

        await sendEditLog(
          `🛠️ AJUSTE MANUAL (00) — ${cfg.NAME}`,
          `👤 Membro: <@${user.id}>\n🧾 Tipo: **SEMENTES**\n✏️ Ajuste: **${valor >= 0 ? "+" : ""}${valor}**\n📌 Antes: **${antes}** → Depois: **${depois}**\n🛡️ Feito por: <@${message.author.id}>`
        );

        return message.reply(`✅ Sementes atualizado: <@${user.id}> **${depois}/100**`);
      }

      if (tipo === "extra_papel") {
        const antes = Number(before.papelCarry || 0);
        const depois = Math.max(0, antes + valor);

        await pool.query(`UPDATE usuarios SET "papelCarry"=$1 WHERE "guildId"=$2 AND "userId"=$3`, [
          depois,
          message.guild.id,
          user.id,
        ]);

        await sendEditLog(
          `🛠️ AJUSTE MANUAL (00) — ${cfg.NAME}`,
          `👤 Membro: <@${user.id}>\n🧾 Tipo: **EXTRA PAPEL**\n✏️ Ajuste: **${valor >= 0 ? "+" : ""}${valor}**\n📌 Antes: **${antes}** → Depois: **${depois}**\n🛡️ Feito por: <@${message.author.id}>`
        );

        return message.reply(`✅ Extra (papel) atualizado: <@${user.id}> **${depois}**`);
      }

      if (tipo === "extra_sementes") {
        const antes = Number(before.sementesCarry || 0);
        const depois = Math.max(0, antes + valor);

        await pool.query(`UPDATE usuarios SET "sementesCarry"=$1 WHERE "guildId"=$2 AND "userId"=$3`, [
          depois,
          message.guild.id,
          user.id,
        ]);

        await sendEditLog(
          `🛠️ AJUSTE MANUAL (00) — ${cfg.NAME}`,
          `👤 Membro: <@${user.id}>\n🧾 Tipo: **EXTRA SEMENTES**\n✏️ Ajuste: **${valor >= 0 ? "+" : ""}${valor}**\n📌 Antes: **${antes}** → Depois: **${depois}**\n🛡️ Feito por: <@${message.author.id}>`
        );

        return message.reply(`✅ Extra (sementes) atualizado: <@${user.id}> **${depois}**`);
      }

      return message.reply("Tipo inválido.\nUse: `papel`, `sementes`, `extra_papel`, `extra_sementes`");
    }

    void isGerente;
  } catch (e) {
    console.error("Erro messageCreate:", e?.stack || e);
    try {
      await message.reply("❌ Erro interno.");
    } catch {}
  }
});

// ==========================
// 🔘 BUTTONS (FIX FINAL)
// ==========================
client.on("interactionCreate", async (interaction) => {
  try {
    if (!interaction.isButton() || !interaction.guild) return;

    const cfg = getCfg(interaction.guild.id);
    if (!cfg) return;

    const member = interaction.member ?? (await interaction.guild.members.fetch(interaction.user.id));
    const isGerente = member.roles.cache.has(cfg.GERENTE_ROLE_ID);
    const is00 = member.roles.cache.has(cfg.ROLE_00_ID);

    console.log(
      "[BTN]",
      "guild=",
      interaction.guild.id,
      "user=",
      interaction.user.id,
      "customId=",
      interaction.customId,
      "is00=",
      is00,
      "isGerente=",
      isGerente
    );

    if (!isGerente && !is00) {
      return interaction.reply({ content: "❌ Sem permissão.", ephemeral: true });
    }

    const logChannel = getLogChannel(interaction.guild);

    const parts = interaction.customId.split("_");
    const acao = parts[0];
    const userId = parts[1];
    const quantidadeStr = parts[2];
    const tipo = parts[3];

    const quantidade = parseInt(quantidadeStr, 10);
    const msgId = interaction.message?.id;

    if (!msgId) return interaction.reply({ content: "⚠️ Não consegui pegar o ID da mensagem.", ephemeral: true });

    // ✅ FIX: evita "Unknown interaction"
    try {
      await interaction.deferUpdate();
    } catch (e) {
      console.log("[DEFER_FAIL]", e?.message || e);
      return;
    }

    if (await alreadyProcessed(interaction.guild.id, msgId)) {
      return interaction.followUp({ content: "⚠️ Esse farme já foi processado.", ephemeral: true });
    }

    if (!["papel", "sementes"].includes(tipo) || isNaN(quantidade) || quantidade <= 0) {
      return interaction.followUp({ content: "⚠️ Dados inválidos nesse botão.", ephemeral: true });
    }

    const sendLogEmbed = async (title, description) => {
      if (!logChannel) return;
      const embed = new EmbedBuilder().setColor("#2b2d31").setTitle(title).setDescription(description).setTimestamp();

      const thumb = getThumb(interaction);
      if (thumb) embed.setThumbnail(thumb);

      try {
        await logChannel.send({ embeds: [embed] });
      } catch (err) {
        console.log("[LOG_SEND_FAIL]", err?.message || err);
        await logChannel.send(description).catch(() => null);
      }
    };

    if (acao === "aprovar") {
      const result = await applyFarm(interaction.guild.id, userId, tipo, quantidade);
      const u = result.user;

      await insertHistorico({
        guildId: interaction.guild.id,
        userId,
        tipo,
        quantidade,
        status: "APROVADO",
        msgId,
        gerenteId: interaction.user.id,
        aplicado: result.aplicado,
        carry: result.carry,
        dia: todayKey(),
      });

      try {
        await interaction.message.edit({
          content:
            `✅ **Aprovado**\n` +
            `📦 ${tipo.toUpperCase()} • ${quantidade}\n` +
            `➡️ Aplicado hoje: **${result.aplicado}** | Extra (amanhã): **${result.carry}**\n\n` +
            `📄 Papel: **${u.papelHoje}/100** (extra: ${u.papelCarry})\n` +
            `🌱 Sementes: **${u.sementesHoje}/100** (extra: ${u.sementesCarry})`,
          components: [],
        });
      } catch (errEdit) {
        console.error("[EDIT_FAIL]", errEdit?.stack || errEdit);
        await interaction.followUp({
          content: `⚠️ Aprovou no banco, mas falhei ao editar a mensagem. Motivo: \`${errEdit?.message || "erro"}\``,
          ephemeral: true,
        });
      }

      const debtLine = formatDebtLine(Number(u.papelDebt || 0), Number(u.sementesDebt || 0));

      await sendLogEmbed(
        `✅ FARME APROVADO — ${cfg.NAME}`,
        `👤 Usuário: <@${userId}>\n` +
          `🧾 Tipo: **${tipo.toUpperCase()}**\n` +
          `📦 Quantidade: **${quantidade}**\n` +
          `➡️ Aplicado: **${result.aplicado}** | Extra: **${result.carry}**\n` +
          (debtLine ? `${debtLine}\n` : "") +
          `🛡️ Aprovado por: <@${interaction.user.id}>`
      );

      return;
    }

    if (acao === "negar") {
      await insertHistorico({
        guildId: interaction.guild.id,
        userId,
        tipo,
        quantidade,
        status: "NEGADO",
        msgId,
        gerenteId: interaction.user.id,
        aplicado: 0,
        carry: 0,
        dia: todayKey(),
      });

      try {
        await interaction.message.edit({
          content: `❌ **Negado**\n📦 ${tipo.toUpperCase()} • ${quantidade}`,
          components: [],
        });
      } catch (errEdit) {
        console.error("[EDIT_FAIL]", errEdit?.stack || errEdit);
        await interaction.followUp({
          content: `⚠️ Negou no banco, mas falhei ao editar a mensagem. Motivo: \`${errEdit?.message || "erro"}\``,
          ephemeral: true,
        });
      }

      await sendLogEmbed(
        `❌ FARME NEGADO — ${cfg.NAME}`,
        `👤 Usuário: <@${userId}>\n` +
          `🧾 Tipo: **${tipo.toUpperCase()}**\n` +
          `📦 Quantidade: **${quantidade}**\n` +
          `🛡️ Negado por: <@${interaction.user.id}>`
      );

      return;
    }

    return interaction.followUp({ content: "⚠️ Ação inválida.", ephemeral: true });
  } catch (e) {
    console.error("Erro interactionCreate:", e?.stack || e);

    try {
      if (interaction.deferred || interaction.replied) {
        await interaction.followUp({ content: "❌ Erro interno.", ephemeral: true });
      } else {
        await interaction.reply({ content: "❌ Erro interno.", ephemeral: true });
      }
    } catch {}
  }
});

// ==========================
// START
// ==========================
(async () => {
  try {
    await initDB();
    await client.login(process.env.DISCORD_TOKEN);
  } catch (e) {
    console.error("Falha ao iniciar:", e?.stack || e);
    process.exit(1);
  }
})();