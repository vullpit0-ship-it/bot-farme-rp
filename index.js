require("dotenv").config();

const express = require("express");
const cron = require("node-cron");
const { Pool } = require("pg");
const { DateTime } = require("luxon");

const {
  Client,
  GatewayIntentBits,
  Partials,
  REST,
  Routes,
  SlashCommandBuilder,
  ActionRowBuilder,
  ChannelType,
  PermissionFlagsBits,
  ButtonBuilder,
  ButtonStyle,
  EmbedBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
} = require("discord.js");

// ======================================================
// 🌐 WEB SERVER (Render)
// ======================================================
const app = express();
app.get("/", (req, res) => res.send("Bot online ✅"));
app.listen(process.env.PORT || 3000, "0.0.0.0", () => {
  console.log(`Web OK na porta ${process.env.PORT || 3000}`);
});

// ======================================================
// ✅ ENV
// ======================================================
if (!process.env.DISCORD_TOKEN) {
  console.error("Faltando DISCORD_TOKEN");
  process.exit(1);
}
if (!process.env.CLIENT_ID) {
  console.error("Faltando CLIENT_ID");
  process.exit(1);
}
if (!process.env.DATABASE_URL) {
  console.error("Faltando DATABASE_URL");
  process.exit(1);
}

process.on("unhandledRejection", (e) => console.error("UNHANDLED REJECTION:", e));
process.on("uncaughtException", (e) => console.error("UNCAUGHT EXCEPTION:", e));

// ======================================================
// 🗄️ POSTGRES / SUPABASE
// ======================================================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ======================================================
// ✅ CONFIG POR SERVIDOR
// ======================================================
const GUILD_ID_TESTE = "1477774289414656213";
const GUILD_ID_NOVA_ORDEM = "1469111028796227728";

const CONFIGS = {
  [GUILD_ID_TESTE]: {
    NAME: "TESTE",

    ROLE_00_ID: "1477850489189044365",
    GERENTE_ROLE_ID: "1477779548484538539",
    ROLE_MEMBRO_ID: "1477868954658541620",

    FARME_CATEGORY_ID: "1478986272520274001",
    LOG_CHANNEL_ID: "1478991741766864906",
    REPORT_CHANNEL_ID: "1479024598166012007",
    STAFF_TABLE_CHANNEL_ID: "1479158423684649167",

    WEEKLY_PANEL_CHANNEL_ID: "1479185447367479389",
    DAILY_PANEL_CHANNEL_ID: "1479185862196461638",

    CLOSED_CATEGORY_ID: "",
    DAILY_DM_WHITELIST: [],
  },

  [GUILD_ID_NOVA_ORDEM]: {
    NAME: "NOVA ORDEM",

    ROLE_00_ID: "1469111029161136392",
    GERENTE_ROLE_ID: "1469111029161136386",
    ROLE_MEMBRO_ID: "1469111029144223913",

    FARME_CATEGORY_ID: "1479371659616981022",
    LOG_CHANNEL_ID: "1478096038114885704",
    REPORT_CHANNEL_ID: "1478556681070706951",
    STAFF_TABLE_CHANNEL_ID: "1479371176529756271",
    WEEKLY_PANEL_CHANNEL_ID: "1479362819639083110",
    DAILY_PANEL_CHANNEL_ID: "1479362995539808286",

    CLOSED_CATEGORY_ID: "",
    DAILY_DM_WHITELIST: [],
  },
};

function getCfg(guildId) {
  return CONFIGS[guildId] || null;
}

// ======================================================
// ⚙️ GERAL
// ======================================================
const TZ = "America/Cuiaba";
const COOLDOWN_SECONDS = 60;
const CLEANUP_EVERY_MS = 6 * 60 * 60 * 1000;
const DELETE_CLOSED_OLDER_THAN_DAYS = 14;
const DELETE_PENDING_OLDER_THAN_DAYS = 7;

const FARME_OPTIONS = [
  { label: "Pasta Base", value: "pasta-base", description: "Pasta Base" },
  { label: "Estabilizador", value: "estabilizador", description: "Estabilizador" },
  { label: "Saco Ziplock", value: "saco-ziplock", description: "Saco Ziplock" },
  { label: "Folha Bruta", value: "folha-bruta", description: "Folha Bruta" },
];

const TOTAL_TABLES = new Set(["farme_daily_totals", "farme_weekly_totals"]);
const PANEL_PREFIX_DAILY = "panel_daily:";
const PANEL_PREFIX_WEEKLY = "panel_weekly:";
const FARM_CHANNEL_ITEMVALUE = "__farm_panel__";

// ======================================================
// 🤖 CLIENT
// ======================================================
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Channel],
});

client.on("error", (e) => console.error("CLIENT ERROR:", e));

// ======================================================
// 🧱 DB INIT
// ======================================================
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS farme_requests (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      "requestId" TEXT NOT NULL,
      "messageId" TEXT NOT NULL,
      "channelId" TEXT NOT NULL,
      "userId" TEXT NOT NULL,
      "userTag" TEXT,
      "itemValue" TEXT NOT NULL,
      "itemLabel" TEXT NOT NULL,
      quantidade INTEGER NOT NULL DEFAULT 0,
      "originalQuantidade" INTEGER NOT NULL DEFAULT 0,
      "printUrl" TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      "createdAt" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      "decidedAt" TIMESTAMPTZ,
      "decidedById" TEXT,
      "decidedByTag" TEXT,
      "denyReason" TEXT,
      "adjustedAt" TIMESTAMPTZ,
      "adjustedById" TEXT,
      "adjustedByTag" TEXT,
      "adjustedDelta" INTEGER NOT NULL DEFAULT 0,
      "adjustedNote" TEXT,
      "closedAt" TIMESTAMPTZ,
      "closedById" TEXT,
      "closedByTag" TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS farme_requests_request_unique
    ON farme_requests ("guildId","requestId");

    CREATE INDEX IF NOT EXISTS idx_farme_requests_channel
    ON farme_requests ("guildId","channelId");

    CREATE INDEX IF NOT EXISTS idx_farme_requests_user
    ON farme_requests ("guildId","userId");

    CREATE INDEX IF NOT EXISTS idx_farme_requests_status
    ON farme_requests ("guildId",status);

    CREATE TABLE IF NOT EXISTS farme_daily_routes (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      "dateKey" TEXT NOT NULL,
      "userId" TEXT NOT NULL,
      "itemValue" TEXT NOT NULL,
      total INTEGER NOT NULL DEFAULT 0,
      UNIQUE ("guildId","dateKey","userId","itemValue")
    );

    CREATE TABLE IF NOT EXISTS farme_daily_totals (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      "userId" TEXT NOT NULL,
      "itemValue" TEXT NOT NULL,
      total INTEGER NOT NULL DEFAULT 0,
      UNIQUE ("guildId","userId","itemValue")
    );

    CREATE TABLE IF NOT EXISTS farme_weekly_totals (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      "userId" TEXT NOT NULL,
      "itemValue" TEXT NOT NULL,
      total INTEGER NOT NULL DEFAULT 0,
      UNIQUE ("guildId","userId","itemValue")
    );

    CREATE TABLE IF NOT EXISTS farme_channels (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      "userId" TEXT NOT NULL,
      "itemValue" TEXT NOT NULL,
      "channelId" TEXT NOT NULL,
      UNIQUE ("guildId","userId","itemValue")
    );

    CREATE TABLE IF NOT EXISTS farme_cooldowns (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      "userId" TEXT NOT NULL,
      "lastAt" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE ("guildId","userId")
    );

    CREATE TABLE IF NOT EXISTS farme_fixed_messages (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      chave TEXT NOT NULL,
      valor TEXT,
      UNIQUE ("guildId", chave)
    );

    CREATE TABLE IF NOT EXISTS farme_drafts (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      "channelId" TEXT NOT NULL,
      "userId" TEXT NOT NULL,
      quantities JSONB NOT NULL DEFAULT '{}'::jsonb,
      "panelMessageId" TEXT,
      "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE ("guildId","channelId","userId")
    );
  `);

  console.log("DB OK");
}

// ======================================================
// 🧠 HELPERS
// ======================================================
function slugUser(u) {
  return (u?.username || "membro").toLowerCase().replace(/[^a-z0-9]/g, "").slice(0, 12);
}

function is00(member, cfg) {
  return !!member?.roles?.cache?.has(cfg.ROLE_00_ID);
}

function isStaff(member, cfg) {
  return !!member?.roles?.cache?.has(cfg.ROLE_00_ID) || !!member?.roles?.cache?.has(cfg.GERENTE_ROLE_ID);
}

function isTrackedMember(member, cfg) {
  if (!member || member.user?.bot) return false;
  const roles = member.roles?.cache;
  return (
    (cfg.ROLE_MEMBRO_ID && roles?.has(cfg.ROLE_MEMBRO_ID)) ||
    (cfg.GERENTE_ROLE_ID && roles?.has(cfg.GERENTE_ROLE_ID)) ||
    (cfg.ROLE_00_ID && roles?.has(cfg.ROLE_00_ID))
  );
}

function parseItemFromChannelName(channelName) {
  if (!channelName?.startsWith("farme-")) return null;
  const parts = channelName.split("-");
  if (parts.length < 3) return null;
  const item = parts.slice(1, parts.length - 1).join("-");
  return FARME_OPTIONS.find((o) => o.value === item) || null;
}

function isFarmPanelChannel(channelName) {
  return !!channelName && channelName.startsWith("farm-") && !channelName.startsWith("farme-");
}

function getItemLabel(itemValue) {
  return FARME_OPTIONS.find((o) => o.value === itemValue)?.label || itemValue;
}

function channelLink(guildId, channelId) {
  return `https://discord.com/channels/${guildId}/${channelId}`;
}

function dateKeyNow() {
  return DateTime.now().setZone(TZ).toISODate();
}

function resolveDateKeyFromOption({ dia, dataStr }) {
  const now = DateTime.now().setZone(TZ);
  if (dia === "hoje") return now.toISODate();
  if (dia === "ontem") return now.minus({ days: 1 }).toISODate();

  const raw = (dataStr || "").trim();
  const dt = DateTime.fromISO(raw, { zone: TZ });
  if (!dt.isValid) return null;
  return dt.toISODate();
}

function daysToMs(d) {
  return d * 24 * 60 * 60 * 1000;
}

function splitIntoPages(rows, maxLen = 3500) {
  const pages = [];
  let current = "";
  for (const r of rows) {
    const add = (current ? "\n\n" : "") + r;
    if ((current + add).length > maxLen) {
      pages.push(current);
      current = r;
    } else {
      current += add;
    }
  }
  if (current) pages.push(current);
  return pages;
}

function makeRequestEmbed({ userTag, userId, itemLabel, quantidade, status, approverTag, reason, adjustedInfo }) {
  const embed = new EmbedBuilder()
    .setTitle("📦 Solicitação de Farme")
    .setDescription("Detalhes da solicitação abaixo.")
    .addFields(
      { name: "👤 Membro", value: `<@${userId}> (${userTag || userId})`, inline: false },
      { name: "🧾 Item", value: itemLabel, inline: true },
      { name: "🔢 Quantidade", value: String(quantidade), inline: true },
      { name: "📌 Status", value: status, inline: true }
    )
    .setTimestamp(new Date());

  if (approverTag) embed.addFields({ name: "✅ Avaliado por", value: approverTag, inline: false });
  if (reason) embed.addFields({ name: "📝 Motivo", value: reason, inline: false });
  if (adjustedInfo) embed.addFields({ name: "🛠️ Ajuste (00)", value: adjustedInfo, inline: false });

  return embed;
}

function publicButtons({ disabled = false } = {}) {
  const approve = new ButtonBuilder()
    .setCustomId("farme_public_aprovar")
    .setLabel("Aprovar")
    .setStyle(ButtonStyle.Success)
    .setDisabled(disabled);

  const deny = new ButtonBuilder()
    .setCustomId("farme_public_negar")
    .setLabel("Negar")
    .setStyle(ButtonStyle.Danger)
    .setDisabled(disabled);

  return new ActionRowBuilder().addComponents(approve, deny);
}

function staffPanelButtons(requestId, canAdjust) {
  const close = new ButtonBuilder()
    .setCustomId(`farme_staff_fechar:${requestId}`)
    .setLabel("Fechar Canal")
    .setStyle(ButtonStyle.Secondary);

  const end = new ButtonBuilder()
    .setCustomId(`farme_staff_encerrar:${requestId}`)
    .setLabel("Encerrar (Deletar)")
    .setStyle(ButtonStyle.Danger);

  const adjust = new ButtonBuilder()
    .setCustomId(`farme_staff_ajustar:${requestId}`)
    .setLabel("Ajustar Farme (00)")
    .setStyle(ButtonStyle.Primary)
    .setDisabled(!canAdjust);

  return new ActionRowBuilder().addComponents(close, end, adjust);
}

function getHelpEmbedFor(member, cfg) {
  const is_00 = is00(member, cfg);
  const is_staff = isStaff(member, cfg);

  const memberCmds = [
    { name: "/farme", desc: "Abre seu canal privado com o painel FARM." },
    { name: "/enviarfarme", desc: "Fluxo antigo/manual. Hoje o ideal é usar o painel do /farme." },
    { name: "/meusfarmes", desc: "Ver seus totais do dia por item." },
    { name: "/ranking", desc: "Ver ranking semanal do servidor." },
    { name: "/ajuda", desc: "Ver os comandos disponíveis." },
  ];

  const gerenteCmds = [
    { name: "/gerenciarcanal", desc: "Painel staff para fechar/encerrar o canal atual." },
    { name: "/testardiario", desc: "Tabela staff por cargos e por data." },
  ];

  const extra00Cmds = [
    { name: "/ajustarrota", desc: "Ajusta a rota do dia manualmente (+, -, set), inclusive negativo." },
    { name: "Ajustar Farme (00)", desc: "No painel do canal, botão Ajustar Farme (00)." },
  ];

  const fmt = (arr) => arr.map((c) => `• **${c.name}** — ${c.desc}`).join("\n");

  return new EmbedBuilder()
    .setTitle(`🧭 Ajuda do Bot — ${cfg.NAME}`)
    .setDescription(is_00 ? "Você é **00**." : is_staff ? "Você é **Gerente/Staff**." : "Você é **Membro**.")
    .addFields({ name: "👤 Comandos de Membro", value: fmt(memberCmds), inline: false })
    .addFields(...(is_staff ? [{ name: "🛡️ Comandos de Staff", value: fmt(gerenteCmds), inline: false }] : []))
    .addFields(...(is_00 ? [{ name: "👑 Extras do 00", value: fmt(extra00Cmds), inline: false }] : []))
    .setTimestamp(new Date());
}

function getEmptyDraftQuantities() {
  const out = {};
  for (const o of FARME_OPTIONS) out[o.value] = 0;
  return out;
}

function normalizeDraftQuantities(raw) {
  const base = getEmptyDraftQuantities();
  if (!raw || typeof raw !== "object") return base;

  for (const o of FARME_OPTIONS) {
    const n = Number(raw[o.value] || 0);
    base[o.value] = Number.isFinite(n) && n >= 0 ? Math.floor(n) : 0;
  }

  return base;
}

function sumDraftQuantities(quantities) {
  return FARME_OPTIONS.reduce((acc, o) => acc + Number(quantities?.[o.value] || 0), 0);
}

function buildFarmPanelEmbed(userId, quantities, hasPrint) {
  const lines = FARME_OPTIONS.map((o) => `**${o.label}:** ${quantities[o.value] || 0}`).join("\n");

  return new EmbedBuilder()
    .setTitle("📦 FARM")
    .setDescription(
      `👤 <@${userId}>\n\n` +
      `${lines}\n\n` +
      `📎 **Print:** ${hasPrint ? "✅ detectado" : "❌ aguardando"}\n\n` +
      `**Como usar:**\n` +
      `1. Clique nos botões para ajustar as quantidades.\n` +
      `2. Envie o print/imagem no canal.\n` +
      `3. Clique em **ENVIAR**.`
    )
    .setTimestamp(new Date());
}

function buildFarmPanelButtons(quantities) {
  const row1 = new ActionRowBuilder().addComponents(
    ...FARME_OPTIONS.map((o) =>
      new ButtonBuilder()
        .setCustomId(`farm_edit:${o.value}`)
        .setLabel(`${o.label} [${quantities[o.value] || 0}]`)
        .setStyle(ButtonStyle.Primary)
    )
  );

  const row2 = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId("farm_send")
      .setLabel("ENVIAR")
      .setStyle(ButtonStyle.Success),
    new ButtonBuilder()
      .setCustomId("farm_clear")
      .setLabel("Limpar")
      .setStyle(ButtonStyle.Secondary)
  );

  return [row1, row2];
}

// ======================================================
// 🧾 DB HELPERS
// ======================================================
async function getRequestByRequestId(guildId, requestId) {
  const { rows } = await pool.query(
    `SELECT * FROM farme_requests WHERE "guildId"=$1 AND "requestId"=$2 LIMIT 1`,
    [guildId, requestId]
  );
  return rows[0] || null;
}

async function getLatestRequestByChannel(guildId, channelId) {
  const { rows } = await pool.query(
    `SELECT * FROM farme_requests
     WHERE "guildId"=$1 AND "channelId"=$2
     ORDER BY "createdAt" DESC, id DESC
     LIMIT 1`,
    [guildId, channelId]
  );
  return rows[0] || null;
}

async function upsertChannelMap(guildId, userId, itemValue, channelId) {
  await pool.query(
    `INSERT INTO farme_channels ("guildId","userId","itemValue","channelId")
     VALUES ($1,$2,$3,$4)
     ON CONFLICT ("guildId","userId","itemValue")
     DO UPDATE SET "channelId"=EXCLUDED."channelId"`,
    [guildId, userId, itemValue, channelId]
  );
}

async function getMappedChannelId(guildId, userId, itemValue) {
  const { rows } = await pool.query(
    `SELECT "channelId"
     FROM farme_channels
     WHERE "guildId"=$1 AND "userId"=$2 AND "itemValue"=$3
     LIMIT 1`,
    [guildId, userId, itemValue]
  );
  return rows[0]?.channelId || null;
}

async function setCooldown(guildId, userId) {
  await pool.query(
    `INSERT INTO farme_cooldowns ("guildId","userId","lastAt")
     VALUES ($1,$2,NOW())
     ON CONFLICT ("guildId","userId")
     DO UPDATE SET "lastAt"=NOW()`,
    [guildId, userId]
  );
}

async function getCooldownRemaining(guildId, userId) {
  const { rows } = await pool.query(
    `SELECT EXTRACT(EPOCH FROM "lastAt") AS epoch
     FROM farme_cooldowns
     WHERE "guildId"=$1 AND "userId"=$2
     LIMIT 1`,
    [guildId, userId]
  );

  const epoch = Number(rows[0]?.epoch || 0);
  if (!epoch) return 0;

  const elapsed = Date.now() / 1000 - epoch;
  const remaining = COOLDOWN_SECONDS - elapsed;
  return remaining > 0 ? Math.ceil(remaining) : 0;
}

async function addPeriodTotal(tableName, guildId, userId, itemValue, quantidade) {
  if (!TOTAL_TABLES.has(tableName)) {
    throw new Error(`Tabela inválida: ${tableName}`);
  }

  await pool.query(
    `INSERT INTO ${tableName} ("guildId","userId","itemValue",total)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT ("guildId","userId","itemValue")
     DO UPDATE SET total = GREATEST(0, ${tableName}.total + EXCLUDED.total)`,
    [guildId, userId, itemValue, quantidade]
  );
}

async function addDailyTotals(guildId, userId, itemValue, quantidade) {
  await addPeriodTotal("farme_daily_totals", guildId, userId, itemValue, quantidade);
}

async function addWeeklyTotals(guildId, userId, itemValue, quantidade) {
  await addPeriodTotal("farme_weekly_totals", guildId, userId, itemValue, quantidade);
}

async function addDailyRouteCount(guildId, userId, itemValue, delta) {
  const dk = dateKeyNow();

  await pool.query(
    `INSERT INTO farme_daily_routes ("guildId","dateKey","userId","itemValue",total)
     VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT ("guildId","dateKey","userId","itemValue")
     DO UPDATE SET total = farme_daily_routes.total + EXCLUDED.total`,
    [guildId, dk, userId, itemValue, delta]
  );
}

async function setDailyRouteCount(guildId, userId, itemValue, value) {
  const dk = dateKeyNow();

  await pool.query(
    `INSERT INTO farme_daily_routes ("guildId","dateKey","userId","itemValue",total)
     VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT ("guildId","dateKey","userId","itemValue")
     DO UPDATE SET total = EXCLUDED.total`,
    [guildId, dk, userId, itemValue, value]
  );
}

async function getApprovedCount(guildId, dateKey, userId, itemValue) {
  const { rows } = await pool.query(
    `SELECT total FROM farme_daily_routes
     WHERE "guildId"=$1 AND "dateKey"=$2 AND "userId"=$3 AND "itemValue"=$4
     LIMIT 1`,
    [guildId, dateKey, userId, itemValue]
  );
  return Number(rows[0]?.total || 0);
}

async function missStreakUntil(guildId, dateKey, userId, itemValue, maxLookbackDays = 120) {
  const base = DateTime.fromISO(dateKey, { zone: TZ });
  if (!base.isValid) return 0;

  const todayCount = await getApprovedCount(guildId, dateKey, userId, itemValue);
  if (todayCount >= 1) return 0;

  let streak = 0;
  for (let i = 0; i < maxLookbackDays; i++) {
    const dk = base.minus({ days: i }).toISODate();
    const n = await getApprovedCount(guildId, dk, userId, itemValue);
    if (n >= 1) break;
    streak++;
  }
  return streak;
}

async function getUserTotalsFromTable(tableName, guildId, userId) {
  if (!TOTAL_TABLES.has(tableName)) {
    throw new Error(`Tabela inválida: ${tableName}`);
  }

  const { rows } = await pool.query(
    `SELECT "itemValue", total
     FROM ${tableName}
     WHERE "guildId"=$1 AND "userId"=$2`,
    [guildId, userId]
  );

  const items = {};
  let total = 0;

  for (const r of rows) {
    items[r.itemValue] = Number(r.total || 0);
    total += Number(r.total || 0);
  }

  return { total, items };
}

async function getUserDailyTotals(guildId, userId) {
  return getUserTotalsFromTable("farme_daily_totals", guildId, userId);
}

async function getUserWeeklyTotals(guildId, userId) {
  return getUserTotalsFromTable("farme_weekly_totals", guildId, userId);
}

async function getRankingWeekly(guildId, limit = 10) {
  const { rows } = await pool.query(
    `
    SELECT "userId", COALESCE(SUM(total),0)::int AS total
    FROM farme_weekly_totals
    WHERE "guildId"=$1
    GROUP BY "userId"
    HAVING COALESCE(SUM(total),0) > 0
    ORDER BY total DESC
    LIMIT $2
    `,
    [guildId, limit]
  );
  return rows;
}

async function setFixedMessage(guildId, chave, valor) {
  await pool.query(
    `INSERT INTO farme_fixed_messages ("guildId",chave,valor)
     VALUES ($1,$2,$3)
     ON CONFLICT ("guildId",chave)
     DO UPDATE SET valor=EXCLUDED.valor`,
    [guildId, chave, valor]
  );
}

async function getFixedMessage(guildId, chave) {
  const { rows } = await pool.query(
    `SELECT valor FROM farme_fixed_messages WHERE "guildId"=$1 AND chave=$2 LIMIT 1`,
    [guildId, chave]
  );
  return rows[0]?.valor || null;
}

async function getFixedMessagesByPrefix(guildId, prefix) {
  const { rows } = await pool.query(
    `SELECT chave, valor
     FROM farme_fixed_messages
     WHERE "guildId"=$1 AND chave LIKE $2`,
    [guildId, `${prefix}%`]
  );
  return rows;
}

async function ensureDraft(guildId, channelId, userId) {
  await pool.query(
    `INSERT INTO farme_drafts ("guildId","channelId","userId",quantities)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT ("guildId","channelId","userId")
     DO NOTHING`,
    [guildId, channelId, userId, getEmptyDraftQuantities()]
  );
}

async function getDraft(guildId, channelId, userId) {
  const { rows } = await pool.query(
    `SELECT *
     FROM farme_drafts
     WHERE "guildId"=$1 AND "channelId"=$2 AND "userId"=$3
     LIMIT 1`,
    [guildId, channelId, userId]
  );

  if (!rows[0]) return null;

  return {
    ...rows[0],
    quantities: normalizeDraftQuantities(rows[0].quantities),
  };
}

async function setDraftQuantity(guildId, channelId, userId, itemValue, quantity) {
  const draft = await getDraft(guildId, channelId, userId);
  const next = normalizeDraftQuantities(draft?.quantities || {});
  next[itemValue] = Math.max(0, Number(quantity || 0));

  await pool.query(
    `UPDATE farme_drafts
     SET quantities=$1, "updatedAt"=NOW()
     WHERE "guildId"=$2 AND "channelId"=$3 AND "userId"=$4`,
    [next, guildId, channelId, userId]
  );
}

async function clearDraft(guildId, channelId, userId) {
  await pool.query(
    `UPDATE farme_drafts
     SET quantities=$1, "updatedAt"=NOW()
     WHERE "guildId"=$2 AND "channelId"=$3 AND "userId"=$4`,
    [getEmptyDraftQuantities(), guildId, channelId, userId]
  );
}

async function setDraftPanelMessageId(guildId, channelId, userId, panelMessageId) {
  await pool.query(
    `UPDATE farme_drafts
     SET "panelMessageId"=$1, "updatedAt"=NOW()
     WHERE "guildId"=$2 AND "channelId"=$3 AND "userId"=$4`,
    [panelMessageId, guildId, channelId, userId]
  );
}

async function sendLog(guild, content, embed) {
  const cfg = getCfg(guild.id);
  if (!cfg?.LOG_CHANNEL_ID) return;

  const logChannel = await guild.channels.fetch(cfg.LOG_CHANNEL_ID).catch(() => null);
  if (logChannel && logChannel.isTextBased()) {
    await logChannel.send({ content, embeds: embed ? [embed] : [] }).catch(() => null);
  }
}

async function sendReport(guild, content, embeds = []) {
  const cfg = getCfg(guild.id);
  if (!cfg?.REPORT_CHANNEL_ID) return;

  const ch = await guild.channels.fetch(cfg.REPORT_CHANNEL_ID).catch(() => null);
  if (ch && ch.isTextBased()) {
    await ch.send({ content, embeds }).catch(() => null);
  }
}

async function sendStaffTable(guild, content, embeds = []) {
  const cfg = getCfg(guild.id);
  if (!cfg?.STAFF_TABLE_CHANNEL_ID) return false;

  const ch = await guild.channels.fetch(cfg.STAFF_TABLE_CHANNEL_ID).catch(() => null);
  if (ch && ch.isTextBased()) {
    await ch.send({ content, embeds }).catch(() => null);
    return true;
  }
  return false;
}

async function safeDailyDM(userId, text) {
  const user = await client.users.fetch(userId).catch(() => null);
  if (!user) return;
  await user.send(text).catch(() => null);
}

async function safeNotifyDM(userId, text) {
  const user = await client.users.fetch(userId).catch(() => null);
  if (!user) return;
  await user.send(text).catch(() => null);
}

async function cleanupDB() {
  try {
    const now = Date.now();
    const closedCutoff = new Date(now - daysToMs(DELETE_CLOSED_OLDER_THAN_DAYS));
    const pendingCutoff = new Date(now - daysToMs(DELETE_PENDING_OLDER_THAN_DAYS));

    await pool.query(
      `DELETE FROM farme_requests
       WHERE status='closed' AND "createdAt" < $1`,
      [closedCutoff]
    );

    await pool.query(
      `DELETE FROM farme_requests
       WHERE status='pending' AND "createdAt" < $1`,
      [pendingCutoff]
    );
  } catch (e) {
    console.error("Cleanup falhou:", e);
  }
}

// ======================================================
// 📦 NOVO PAINEL FARM
// ======================================================
async function findLatestImageFromUser(channel, userId, limit = 30) {
  const msgs = await channel.messages.fetch({ limit }).catch(() => null);
  if (!msgs) return null;

  for (const msg of msgs.values()) {
    if (msg.author?.id !== userId) continue;
    const img = msg.attachments.find((att) => att.contentType?.startsWith("image/"));
    if (img) return img.url;
  }

  return null;
}

async function sendOrRefreshFarmPanel(channel, userId) {
  await ensureDraft(channel.guild.id, channel.id, userId);
  const draft = await getDraft(channel.guild.id, channel.id, userId);
  if (!draft) return;

  const printUrl = await findLatestImageFromUser(channel, userId);
  const embed = buildFarmPanelEmbed(userId, draft.quantities, !!printUrl);
  const components = buildFarmPanelButtons(draft.quantities);

  if (draft.panelMessageId) {
    const oldMsg = await channel.messages.fetch(draft.panelMessageId).catch(() => null);
    if (oldMsg) {
      await oldMsg.edit({ content: null, embeds: [embed], components }).catch(() => null);
      return oldMsg;
    }
  }

  const created = await channel.send({
    content: `👋 <@${userId}> seu painel FARM está pronto.`,
    embeds: [embed],
    components,
  }).catch(() => null);

  if (created) {
    await setDraftPanelMessageId(channel.guild.id, channel.id, userId, created.id);
  }

  return created;
}

async function createOrGetFarmChannel(guild, user, cfg) {
  const mappedId = await getMappedChannelId(guild.id, user.id, FARM_CHANNEL_ITEMVALUE);

  if (mappedId) {
    const existingByMap = await guild.channels.fetch(mappedId).catch(() => null);
    if (existingByMap && existingByMap.type === ChannelType.GuildText) {
      return existingByMap;
    }
  }

  const expectedName = `farm-${slugUser(user)}`.slice(0, 90);

  let targetChannel = guild.channels.cache.find(
    (c) => c.type === ChannelType.GuildText && c.name === expectedName
  );

  if (!targetChannel) {
    targetChannel = await guild.channels.create({
      name: expectedName,
      type: ChannelType.GuildText,
      parent: cfg.FARME_CATEGORY_ID || null,
      permissionOverwrites: [
        { id: guild.id, deny: [PermissionFlagsBits.ViewChannel] },
        {
          id: user.id,
          allow: [
            PermissionFlagsBits.ViewChannel,
            PermissionFlagsBits.SendMessages,
            PermissionFlagsBits.ReadMessageHistory,
            PermissionFlagsBits.AttachFiles,
          ],
        },
        {
          id: cfg.ROLE_00_ID,
          allow: [
            PermissionFlagsBits.ViewChannel,
            PermissionFlagsBits.SendMessages,
            PermissionFlagsBits.ReadMessageHistory,
            PermissionFlagsBits.ManageMessages,
            PermissionFlagsBits.ManageChannels,
          ],
        },
        {
          id: cfg.GERENTE_ROLE_ID,
          allow: [
            PermissionFlagsBits.ViewChannel,
            PermissionFlagsBits.SendMessages,
            PermissionFlagsBits.ReadMessageHistory,
            PermissionFlagsBits.ManageMessages,
            PermissionFlagsBits.ManageChannels,
          ],
        },
      ],
      topic: `Painel FARM de ${user.tag}`,
    });
  }

  await upsertChannelMap(guild.id, user.id, FARM_CHANNEL_ITEMVALUE, targetChannel.id);
  await ensureDraft(guild.id, targetChannel.id, user.id);

  return targetChannel;
}

async function createPendingRequestsFromDraft({ guild, channel, user, quantities, printUrl }) {
  const created = [];

  for (const opt of FARME_OPTIONS) {
    const quantidade = Number(quantities?.[opt.value] || 0);
    if (quantidade <= 0) continue;

    const embed = makeRequestEmbed({
      userTag: user.tag,
      userId: user.id,
      itemLabel: opt.label,
      quantidade,
      status: "🟡 Pendente",
    }).setImage(printUrl);

    const msg = await channel.send({
      content: `📣 Solicitação enviada por ${user}. (Aguardando **00/Gerente**)`,
      embeds: [embed],
      components: [publicButtons({ disabled: false })],
    });

    await pool.query(
      `INSERT INTO farme_requests
      ("guildId","requestId","messageId","channelId","userId","userTag","itemValue","itemLabel",quantidade,"originalQuantidade","printUrl",status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending')`,
      [
        guild.id,
        msg.id,
        msg.id,
        channel.id,
        user.id,
        user.tag,
        opt.value,
        opt.label,
        quantidade,
        quantidade,
        printUrl,
      ]
    );

    created.push({
      requestId: msg.id,
      itemValue: opt.value,
      itemLabel: opt.label,
      quantidade,
    });
  }

  return created;
}

// ======================================================
// 📊 PAINÉIS FIXOS POR MEMBRO
// ======================================================
function buildQuantityPanelEmbed({ title, userId, totals, footerText }) {
  const lines = FARME_OPTIONS.map((o) => `• **${o.label}:** ${totals.items[o.value] || 0}`).join("\n");

  return new EmbedBuilder()
    .setTitle(title)
    .setDescription(`👤 <@${userId}>\n\n${lines}`)
    .addFields({ name: "🏁 Total", value: String(totals.total || 0), inline: true })
    .setFooter({ text: footerText })
    .setTimestamp(new Date());
}

async function upsertUserPanelMessage({
  guild,
  channelId,
  keyPrefix,
  userId,
  embed,
  content = null,
}) {
  if (!channelId) return;

  const channel = await guild.channels.fetch(channelId).catch(() => null);
  if (!channel || !channel.isTextBased()) return;

  const key = `${keyPrefix}${userId}`;
  const existingId = await getFixedMessage(guild.id, key);

  if (existingId) {
    const msg = await channel.messages.fetch(existingId).catch(() => null);
    if (msg) {
      await msg.edit({ content, embeds: [embed] }).catch(() => null);
      return;
    }
  }

  const created = await channel.send({ content, embeds: [embed] }).catch(() => null);
  if (created) {
    await setFixedMessage(guild.id, key, created.id);
  }
}

async function updateDailyPanelFor(guild, userId) {
  const cfg = getCfg(guild.id);
  if (!cfg?.DAILY_PANEL_CHANNEL_ID) return;

  const totals = await getUserDailyTotals(guild.id, userId);
  const embed = buildQuantityPanelEmbed({
    title: "📊 Painel Diário de Produção",
    userId,
    totals,
    footerText: `Zera todo dia às 00:05 • ${guild.name}`,
  });

  await upsertUserPanelMessage({
    guild,
    channelId: cfg.DAILY_PANEL_CHANNEL_ID,
    keyPrefix: PANEL_PREFIX_DAILY,
    userId,
    embed,
  });
}

async function updateWeeklyPanelFor(guild, userId) {
  const cfg = getCfg(guild.id);
  if (!cfg?.WEEKLY_PANEL_CHANNEL_ID) return;

  const totals = await getUserWeeklyTotals(guild.id, userId);
  const embed = buildQuantityPanelEmbed({
    title: "🏆 Painel Semanal de Produção",
    userId,
    totals,
    footerText: `Zera toda segunda às 00:05 • ${guild.name}`,
  });

  await upsertUserPanelMessage({
    guild,
    channelId: cfg.WEEKLY_PANEL_CHANNEL_ID,
    keyPrefix: PANEL_PREFIX_WEEKLY,
    userId,
    embed,
  });
}

async function refreshAllExistingPanelsByPrefix(guild, prefix, updaterFn) {
  const rows = await getFixedMessagesByPrefix(guild.id, prefix);
  for (const row of rows) {
    const userId = row.chave.replace(prefix, "");
    if (userId) {
      await updaterFn(guild, userId).catch(() => null);
    }
  }
}

async function updatePanelsAfterChange(guild, userId) {
  await updateDailyPanelFor(guild, userId).catch(() => null);
  await updateWeeklyPanelFor(guild, userId).catch(() => null);
}

// ======================================================
// 📅 RELATÓRIOS / RESETS
// ======================================================
async function buildStaffRow(guildId, userId, displayName, dateKey) {
  const doneParts = [];
  const streakParts = [];

  for (const o of FARME_OPTIONS) {
    const done = await getApprovedCount(guildId, dateKey, userId, o.value);
    const streak = await missStreakUntil(guildId, dateKey, userId, o.value);
    doneParts.push(`${o.label}: ${done}`);
    streakParts.push(`${o.label}: ${streak}`);
  }

  return (
    `👤 **${displayName}** (<@${userId}>)\n` +
    `✅ **Rotas no dia:** ${doneParts.join(" | ")}\n` +
    `📌 **Rota completa:** ${streakParts.join(" | ")}`
  );
}

async function runDailyAuditAndReport() {
  const dk = DateTime.now().setZone(TZ).minus({ days: 1 }).toISODate();

  for (const guild of client.guilds.cache.values()) {
    const cfg = getCfg(guild.id);
    if (!cfg) continue;

    for (const userId of cfg.DAILY_DM_WHITELIST || []) {
      await safeDailyDM(
        userId,
        `📅 **Meta diária (${dk})**\n\n📌 Use **/testardiario** para ver a tabela completa no canal staff.`
      );
    }

    await sendReport(guild, `✅ Relatório diário gerado (${dk}).`);
  }
}

async function resetDailyProductivity(guild) {
  await pool.query(`DELETE FROM farme_daily_totals WHERE "guildId"=$1`, [guild.id]);
  await refreshAllExistingPanelsByPrefix(guild, PANEL_PREFIX_DAILY, updateDailyPanelFor);
}

async function sendWeeklyRankingReportAndReset(guild) {
  const ranking = await getRankingWeekly(guild.id, 10);

  const now = DateTime.now().setZone(TZ);
  const weekEnd = now.minus({ days: 1 });
  const weekStart = weekEnd.minus({ days: 6 });
  const periodText = `${weekStart.toFormat("dd/MM/yyyy")} até ${weekEnd.toFormat("dd/MM/yyyy")}`;

  const desc = ranking.length
    ? ranking
        .map((e, i) => {
          const medal = i === 0 ? "🥇" : i === 1 ? "🥈" : i === 2 ? "🥉" : "🔸";
          return `${medal} **${i + 1}.** <@${e.userId}> — **${e.total}**`;
        })
        .join("\n")
    : "Ninguém pontuou nesta semana.";

  const winnerText = ranking.length ? `<@${ranking[0].userId}>` : "Sem vencedor";

  const embed = new EmbedBuilder()
    .setTitle("🏁 Fechamento Semanal de Farmes")
    .setDescription(desc)
    .addFields(
      { name: "📆 Período", value: periodText, inline: false },
      { name: "👑 Vencedor", value: winnerText, inline: false }
    )
    .setTimestamp(new Date());

  await sendStaffTable(
    guild,
    `📣 **Relatório semanal fechado automaticamente**\nServidor: **${guild.name}**`,
    [embed]
  );

  await pool.query(`DELETE FROM farme_weekly_totals WHERE "guildId"=$1`, [guild.id]);
  await refreshAllExistingPanelsByPrefix(guild, PANEL_PREFIX_WEEKLY, updateWeeklyPanelFor);
}

async function dailySchedulerRun() {
  const now = DateTime.now().setZone(TZ);

  for (const guild of client.guilds.cache.values()) {
    if (!getCfg(guild.id)) continue;

    if (now.weekday === 1) {
      await sendWeeklyRankingReportAndReset(guild).catch(() => null);
    }

    await resetDailyProductivity(guild).catch(() => null);
  }

  await runDailyAuditAndReport().catch(() => null);
}

// ======================================================
// 🛠️ SLASH COMMANDS
// ======================================================
const commands = [
  new SlashCommandBuilder().setName("farme").setDescription("Abre seu canal privado com o painel FARM."),

  new SlashCommandBuilder()
    .setName("enviarfarme")
    .setDescription("Fluxo antigo/manual: enviar seu farme (quantidade + print) para aprovação.")
    .addIntegerOption((opt) =>
      opt.setName("quantidade").setDescription("Quantidade farmada").setRequired(true).setMinValue(1)
    )
    .addAttachmentOption((opt) =>
      opt.setName("print").setDescription("Envie o print/anexo como prova").setRequired(true)
    ),

  new SlashCommandBuilder().setName("meusfarmes").setDescription("Mostra seus totais do dia por item."),

  new SlashCommandBuilder().setName("ranking").setDescription("Mostra o ranking semanal de farmes (top 10)."),

  new SlashCommandBuilder()
    .setName("gerenciarcanal")
    .setDescription("(Staff) Abre painel para Fechar/Encerrar/Ajustar o canal atual."),

  new SlashCommandBuilder()
    .setName("testardiario")
    .setDescription("(Staff) Posta no canal staff a tabela por cargos.")
    .addStringOption((opt) =>
      opt
        .setName("dia")
        .setDescription('Escolha: "hoje", "ontem" ou "data"')
        .setRequired(true)
        .addChoices(
          { name: "hoje", value: "hoje" },
          { name: "ontem", value: "ontem" },
          { name: "data (YYYY-MM-DD)", value: "data" }
        )
    )
    .addStringOption((opt) =>
      opt.setName("data").setDescription('Se "dia" = data, coloque aqui: YYYY-MM-DD').setRequired(false)
    ),

  new SlashCommandBuilder()
    .setName("ajustarrota")
    .setDescription("(00) Ajusta a rota do dia manualmente.")
    .addUserOption((opt) =>
      opt.setName("membro").setDescription("Membro que será ajustado").setRequired(true)
    )
    .addStringOption((opt) =>
      opt
        .setName("item")
        .setDescription("Item/rota")
        .setRequired(true)
        .addChoices(
          ...FARME_OPTIONS.map((o) => ({ name: o.label, value: o.value }))
        )
    )
    .addStringOption((opt) =>
      opt
        .setName("operacao")
        .setDescription('Operação: "+", "-", ou "set"')
        .setRequired(true)
        .addChoices(
          { name: "+", value: "+" },
          { name: "-", value: "-" },
          { name: "set", value: "set" }
        )
    )
    .addIntegerOption((opt) =>
      opt.setName("valor").setDescription("Valor do ajuste").setRequired(true)
    ),

  new SlashCommandBuilder().setName("ajuda").setDescription("Mostra os comandos disponíveis para o seu cargo."),
].map((c) => c.toJSON());

async function registerCommands() {
  try {
    const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN);

    for (const guildId of Object.keys(CONFIGS)) {
      await rest.put(Routes.applicationGuildCommands(process.env.CLIENT_ID, guildId), { body: commands });
    }
  } catch (err) {
    console.error("ERRO ao registrar commands:", err?.rawError || err);
  }
}

// ======================================================
// READY
// ======================================================
client.once("clientReady", async () => {
  console.log(`BOT ONLINE: ${client.user.tag}`);

  await cleanupDB().catch(() => null);
  setInterval(() => cleanupDB().catch(() => null), CLEANUP_EVERY_MS);

  cron.schedule(
    "5 3 * * *",
    async () => {
      await dailySchedulerRun().catch((e) => console.error("Daily scheduler error:", e));
    },
    { timezone: TZ }
  );

  console.log(`Jobs ativos: reset diário 00:05 e reset semanal segunda 00:05 (${TZ})`);
});

// ======================================================
// INTERACTIONS
// ======================================================
client.on("interactionCreate", async (interaction) => {
  try {
    if (!interaction.guild) {
      if (interaction.isRepliable()) {
        return interaction.reply({ content: "❌ Use isso dentro de um servidor.", ephemeral: true }).catch(() => null);
      }
      return;
    }

    const cfg = getCfg(interaction.guild.id);
    if (!cfg) {
      if (interaction.isRepliable()) {
        return interaction.reply({ content: "❌ Esse servidor não está configurado.", ephemeral: true }).catch(() => null);
      }
      return;
    }

    const member = await interaction.guild.members.fetch(interaction.user.id).catch(() => null);

    // /ajuda
    if (interaction.isChatInputCommand() && interaction.commandName === "ajuda") {
      await interaction.deferReply({ ephemeral: true });
      const embed = getHelpEmbedFor(member, cfg);
      return interaction.editReply({ embeds: [embed] });
    }

    // /testardiario
    if (interaction.isChatInputCommand() && interaction.commandName === "testardiario") {
      await interaction.deferReply({ ephemeral: true });

      if (!isStaff(member, cfg)) {
        return interaction.editReply({ content: "❌ Apenas 00/Gerente." });
      }

      const dia = interaction.options.getString("dia", true);
      const dataStr = interaction.options.getString("data", false);

      const dk = resolveDateKeyFromOption({ dia, dataStr });
      if (!dk) return interaction.editReply('❌ Data inválida. Use: **YYYY-MM-DD**.');

      await interaction.guild.members.fetch().catch(() => null);

      const targets = interaction.guild.members.cache
        .filter((m) => isTrackedMember(m, cfg))
        .sort((a, b) => (a.displayName || a.user.username).localeCompare(b.displayName || b.user.username));

      const rows = [];
      for (const m of targets.values()) {
        rows.push(await buildStaffRow(interaction.guild.id, m.user.id, m.displayName || m.user.username, dk));
      }

      const trackedRolesTxt = [
        cfg.ROLE_MEMBRO_ID ? `<@&${cfg.ROLE_MEMBRO_ID}>` : null,
        cfg.GERENTE_ROLE_ID ? `<@&${cfg.GERENTE_ROLE_ID}>` : null,
        cfg.ROLE_00_ID ? `<@&${cfg.ROLE_00_ID}>` : null,
      ].filter(Boolean).join(" | ");

      const header =
        `📌 **Tabela de metas** solicitada por <@${interaction.user.id}> — Data: **${dk}**\n` +
        `Cargos: ${trackedRolesTxt || "não configurados"}\n` +
        `Total encontrados: **${targets.size}**\n` +
        `Legenda: **Rotas no dia** = aprovações do item no dia (pode ficar negativo se o 00 ajustar) | **Rota completa** = dias seguidos faltando (0 se fez no dia).`;

      const pages = splitIntoPages(rows.length ? rows : ["⚠️ Ninguém encontrado nesses cargos."], 3500);
      const embeds = pages.slice(0, 8).map((txt, idx) =>
        new EmbedBuilder()
          .setTitle(`📊 Controle de metas (${dk})${pages.length > 1 ? ` — Parte ${idx + 1}/${pages.length}` : ""}`)
          .setDescription(txt)
          .setTimestamp(new Date())
      );

      const ok = await sendStaffTable(interaction.guild, header, embeds);
      if (!ok) return interaction.editReply("❌ STAFF_TABLE_CHANNEL_ID não configurado nesse servidor.");

      return interaction.editReply("✅ Postei a tabela no canal staff.");
    }

    // /ajustarrota
    if (interaction.isChatInputCommand() && interaction.commandName === "ajustarrota") {
      await interaction.deferReply({ ephemeral: true });

      if (!is00(member, cfg)) {
        return interaction.editReply("❌ Apenas o **00** pode ajustar rota.");
      }

      const targetUser = interaction.options.getUser("membro", true);
      const itemValue = interaction.options.getString("item", true);
      const operacao = interaction.options.getString("operacao", true);
      const valor = interaction.options.getInteger("valor", true);

      if (targetUser.bot) {
        return interaction.editReply("❌ Não pode ajustar rota de bot.");
      }

      let delta = 0;
      let novo = 0;
      const atual = await getApprovedCount(interaction.guild.id, dateKeyNow(), targetUser.id, itemValue);

      if (operacao === "+") {
        delta = valor;
        novo = atual + delta;
        await addDailyRouteCount(interaction.guild.id, targetUser.id, itemValue, delta);
      } else if (operacao === "-") {
        delta = -valor;
        novo = atual + delta;
        await addDailyRouteCount(interaction.guild.id, targetUser.id, itemValue, delta);
      } else if (operacao === "set") {
        novo = valor;
        delta = novo - atual;
        await setDailyRouteCount(interaction.guild.id, targetUser.id, itemValue, novo);
      } else {
        return interaction.editReply("❌ Operação inválida.");
      }

      const itemLabel = getItemLabel(itemValue);

      await sendLog(
        interaction.guild,
        `🛠️ Ajuste de rota do dia | 00: <@${interaction.user.id}> | Membro: <@${targetUser.id}> | Item: **${itemLabel}** | Operação: **${operacao} ${valor}** | Resultado: **${novo}**`
      );

      return interaction.editReply(
        `✅ Rota ajustada.\nMembro: <@${targetUser.id}>\nItem: **${itemLabel}**\nAntes: **${atual}**\nDepois: **${novo}**`
      );
    }

    // /farme -> abre canal privado único + painel FARM
    if (interaction.isChatInputCommand() && interaction.commandName === "farme") {
      await interaction.deferReply({ ephemeral: true });

      if (!cfg.FARME_CATEGORY_ID) {
        return interaction.editReply({ content: "❌ FARME_CATEGORY_ID não configurado neste servidor." });
      }

      const targetChannel = await createOrGetFarmChannel(interaction.guild, interaction.user, cfg);
      await sendOrRefreshFarmPanel(targetChannel, interaction.user.id);

      const goBtn = new ButtonBuilder()
        .setStyle(ButtonStyle.Link)
        .setLabel("➡️ Ir pro canal")
        .setURL(channelLink(interaction.guild.id, targetChannel.id));

      return interaction.editReply({
        content: "✅ Seu painel FARM está pronto.",
        components: [new ActionRowBuilder().addComponents(goBtn)],
      });
    }

    // /gerenciarcanal
    if (interaction.isChatInputCommand() && interaction.commandName === "gerenciarcanal") {
      await interaction.deferReply({ ephemeral: true });

      if (!isStaff(member, cfg)) {
        return interaction.editReply({ content: "❌ Apenas 00/Gerente." });
      }

      const latest = await getLatestRequestByChannel(interaction.guild.id, interaction.channelId);
      if (!latest) {
        return interaction.editReply({ content: "❌ Não encontrei nenhum pedido salvo para este canal." });
      }

      return interaction.editReply({
        content: `🛠️ Painel do canal atual (Pedido: ${latest.status})`,
        components: [staffPanelButtons(latest.requestId, is00(member, cfg))],
      });
    }

    // /enviarfarme (legado)
    if (interaction.isChatInputCommand() && interaction.commandName === "enviarfarme") {
      await interaction.deferReply({ ephemeral: true });

      if (isFarmPanelChannel(interaction.channel?.name)) {
        return interaction.editReply({
          content: "❌ Neste canal use o **painel FARM** com o botão **ENVIAR**.",
        });
      }

      const opt = parseItemFromChannelName(interaction.channel?.name);
      if (!opt) {
        return interaction.editReply({
          content: "❌ Use este comando dentro do seu canal antigo de farme (farme-...-seunome) ou use **/farme**.",
        });
      }

      const remaining = await getCooldownRemaining(interaction.guild.id, interaction.user.id);
      if (remaining > 0) {
        return interaction.editReply({ content: `⏳ Aguarde **${remaining}s** para enviar outro farme.` });
      }

      const quantidade = interaction.options.getInteger("quantidade", true);
      const print = interaction.options.getAttachment("print", true);

      if (!print.contentType?.startsWith("image/")) {
        return interaction.editReply({ content: "❌ O anexo precisa ser uma **imagem/print**." });
      }

      await setCooldown(interaction.guild.id, interaction.user.id);

      const embed = makeRequestEmbed({
        userTag: interaction.user.tag,
        userId: interaction.user.id,
        itemLabel: opt.label,
        quantidade,
        status: "🟡 Pendente",
      }).setImage(print.url);

      const msg = await interaction.channel.send({
        content: `📣 Solicitação enviada por ${interaction.user}. (Aguardando **00/Gerente**)`,
        embeds: [embed],
        components: [publicButtons({ disabled: false })],
      });

      const requestId = msg.id;

      await pool.query(
        `INSERT INTO farme_requests
        ("guildId","requestId","messageId","channelId","userId","userTag","itemValue","itemLabel",quantidade,"originalQuantidade","printUrl",status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending')`,
        [
          interaction.guild.id,
          requestId,
          msg.id,
          interaction.channel.id,
          interaction.user.id,
          interaction.user.tag,
          opt.value,
          opt.label,
          quantidade,
          quantidade,
          print.url,
        ]
      );

      return interaction.editReply({ content: "✅ Enviado! Aguarde aprovação do 00/Gerente." });
    }

    // /meusfarmes
    if (interaction.isChatInputCommand() && interaction.commandName === "meusfarmes") {
      await interaction.deferReply({ ephemeral: true });

      const totals = await getUserDailyTotals(interaction.guild.id, interaction.user.id);
      const lines = FARME_OPTIONS.map((o) => `• **${o.label}**: ${totals.items[o.value] || 0}`).join("\n");

      const embed = new EmbedBuilder()
        .setTitle("📊 Sua Tabela de Farmes (Hoje)")
        .setDescription(lines)
        .addFields({ name: "🏁 Total do dia", value: String(totals.total), inline: false })
        .setTimestamp(new Date());

      return interaction.editReply({ embeds: [embed] });
    }

    // /ranking
    if (interaction.isChatInputCommand() && interaction.commandName === "ranking") {
      await interaction.deferReply({ ephemeral: true });

      const entries = await getRankingWeekly(interaction.guild.id, 10);
      if (!entries.length) {
        return interaction.editReply({ content: "Ainda não tem farmes aprovados nesta semana." });
      }

      const desc = entries.map((e, i) => `**${i + 1}.** <@${e.userId}> — **${e.total}**`).join("\n");
      const embed = new EmbedBuilder()
        .setTitle("🏆 Ranking Semanal de Farmes (Top 10)")
        .setDescription(desc)
        .setTimestamp(new Date());

      return interaction.editReply({ embeds: [embed] });
    }

    // BOTÕES DO PAINEL FARM
    if (interaction.isButton() && interaction.customId.startsWith("farm_edit:")) {
      const itemValue = interaction.customId.split(":")[1];
      const opt = FARME_OPTIONS.find((o) => o.value === itemValue);

      if (!opt) {
        return interaction.reply({ content: "❌ Item inválido.", ephemeral: true });
      }

      const modal = new ModalBuilder()
        .setCustomId(`farm_modal_qty:${itemValue}`)
        .setTitle(`Quantidade - ${opt.label}`);

      const qtyInput = new TextInputBuilder()
        .setCustomId("qty")
        .setLabel(`Digite a quantidade de ${opt.label}`)
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setMaxLength(10);

      modal.addComponents(new ActionRowBuilder().addComponents(qtyInput));
      return interaction.showModal(modal);
    }

    if (interaction.isButton() && interaction.customId === "farm_clear") {
      await interaction.deferReply({ ephemeral: true });

      await ensureDraft(interaction.guild.id, interaction.channelId, interaction.user.id);
      await clearDraft(interaction.guild.id, interaction.channelId, interaction.user.id);
      await sendOrRefreshFarmPanel(interaction.channel, interaction.user.id);

      return interaction.editReply("✅ Painel limpo.");
    }

    if (interaction.isButton() && interaction.customId === "farm_send") {
      await interaction.deferReply({ ephemeral: true });

      const remaining = await getCooldownRemaining(interaction.guild.id, interaction.user.id);
      if (remaining > 0) {
        return interaction.editReply({ content: `⏳ Aguarde **${remaining}s** para enviar outro farme.` });
      }

      await ensureDraft(interaction.guild.id, interaction.channelId, interaction.user.id);
      const draft = await getDraft(interaction.guild.id, interaction.channelId, interaction.user.id);
      if (!draft) return interaction.editReply("❌ Draft não encontrado.");

      const total = sumDraftQuantities(draft.quantities);
      if (total <= 0) {
        return interaction.editReply("❌ Preencha pelo menos uma quantidade antes de enviar.");
      }

      const printUrl = await findLatestImageFromUser(interaction.channel, interaction.user.id);
      if (!printUrl) {
        return interaction.editReply("❌ Envie o **print/imagem** no canal antes de clicar em **ENVIAR**.");
      }

      await setCooldown(interaction.guild.id, interaction.user.id);

      const created = await createPendingRequestsFromDraft({
        guild: interaction.guild,
        channel: interaction.channel,
        user: interaction.user,
        quantities: draft.quantities,
        printUrl,
      });

      if (!created.length) {
        return interaction.editReply("❌ Nada foi enviado.");
      }

      await clearDraft(interaction.guild.id, interaction.channelId, interaction.user.id);
      await sendOrRefreshFarmPanel(interaction.channel, interaction.user.id);

      const resumo = created.map((x) => `• **${x.itemLabel}**: ${x.quantidade}`).join("\n");

      return interaction.editReply(
        `✅ Enviado para avaliação do **00/Gerente**.\n\n${resumo}`
      );
    }

    // MODAL QUANTIDADE DO PAINEL FARM
    if (interaction.isModalSubmit() && interaction.customId.startsWith("farm_modal_qty:")) {
      await interaction.deferReply({ ephemeral: true });

      const itemValue = interaction.customId.split(":")[1];
      const opt = FARME_OPTIONS.find((o) => o.value === itemValue);

      if (!opt) return interaction.editReply("❌ Item inválido.");

      const qty = parseInt((interaction.fields.getTextInputValue("qty") || "").trim(), 10);
      if (!Number.isFinite(qty) || qty < 0) {
        return interaction.editReply("❌ Quantidade inválida. Use um número 0 ou maior.");
      }

      await ensureDraft(interaction.guild.id, interaction.channelId, interaction.user.id);
      await setDraftQuantity(interaction.guild.id, interaction.channelId, interaction.user.id, itemValue, qty);
      await sendOrRefreshFarmPanel(interaction.channel, interaction.user.id);

      return interaction.editReply(`✅ **${opt.label}** atualizado para **${qty}**.`);
    }

    // BOTÃO APROVAR
    if (interaction.isButton() && interaction.customId === "farme_public_aprovar") {
      await interaction.deferReply({ ephemeral: true });

      if (!isStaff(member, cfg)) {
        return interaction.editReply("❌ Apenas **00** ou **Gerente** pode aprovar/negar.");
      }

      const requestId = interaction.message.id;
      const req = await getRequestByRequestId(interaction.guild.id, requestId);
      if (!req) return interaction.editReply("❌ Não encontrei essa solicitação no banco.");

      if (req.userId === interaction.user.id) {
        return interaction.editReply("❌ Você não pode **aprovar seu próprio farme**.");
      }

      if (req.status !== "pending") {
        return interaction.editReply({
          content: "⚠️ Esse pedido já foi avaliado. Painel staff:",
          components: [staffPanelButtons(requestId, is00(member, cfg))],
        });
      }

      await pool.query(
        `UPDATE farme_requests
         SET status='approved',
             "decidedAt"=NOW(),
             "decidedById"=$1,
             "decidedByTag"=$2
         WHERE "guildId"=$3 AND "requestId"=$4`,
        [interaction.user.id, interaction.user.tag, interaction.guild.id, requestId]
      );

      await addDailyTotals(interaction.guild.id, req.userId, req.itemValue, req.quantidade);
      await addWeeklyTotals(interaction.guild.id, req.userId, req.itemValue, req.quantidade);
      await addDailyRouteCount(interaction.guild.id, req.userId, req.itemValue, 1);

      const embed = makeRequestEmbed({
        userTag: req.userTag,
        userId: req.userId,
        itemLabel: req.itemLabel,
        quantidade: req.quantidade,
        status: "🟢 Aprovado",
        approverTag: interaction.user.tag,
        adjustedInfo: req.adjustedNote || null,
      }).setImage(req.printUrl);

      await interaction.message
        .edit({
          content: `📣 Pedido aprovado por **${interaction.user.tag}**.`,
          embeds: [embed],
          components: [publicButtons({ disabled: true })],
        })
        .catch(() => null);

      await safeNotifyDM(
        req.userId,
        `✅ Seu farme foi **APROVADO**.\nItem: **${req.itemLabel}**\nQuantidade: **${req.quantidade}**\nAprovado por: **${interaction.user.tag}**`
      );

      await sendLog(
        interaction.guild,
        `🟢 Aprovado | Membro: <@${req.userId}> | Por: <@${interaction.user.id}>\nItem: **${req.itemLabel}** | Quantidade: **${req.quantidade}** | Rota do dia +1`,
        embed
      );

      await updatePanelsAfterChange(interaction.guild, req.userId);

      return interaction.editReply({
        content: "✅ Aprovado. Painel staff:",
        components: [staffPanelButtons(requestId, is00(member, cfg))],
      });
    }

    // BOTÃO NEGAR
    if (interaction.isButton() && interaction.customId === "farme_public_negar") {
      if (!isStaff(member, cfg)) {
        return interaction.reply({ content: "❌ Apenas **00** ou **Gerente** pode aprovar/negar.", ephemeral: true });
      }

      const requestId = interaction.message.id;
      const req = await getRequestByRequestId(interaction.guild.id, requestId);
      if (!req) return interaction.reply({ content: "❌ Pedido não encontrado.", ephemeral: true });

      if (req.userId === interaction.user.id) {
        return interaction.reply({ content: "❌ Você não pode **negar seu próprio farme**.", ephemeral: true });
      }

      if (req.status !== "pending") {
        return interaction.reply({
          content: "⚠️ Esse pedido já foi avaliado.",
          components: [staffPanelButtons(requestId, is00(member, cfg))],
          ephemeral: true,
        });
      }

      const modal = new ModalBuilder().setCustomId(`farme_modal_negar:${requestId}`).setTitle("Motivo da negação");

      const reasonInput = new TextInputBuilder()
        .setCustomId("deny_reason")
        .setLabel("Explique o motivo (obrigatório)")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true)
        .setMaxLength(500);

      modal.addComponents(new ActionRowBuilder().addComponents(reasonInput));
      return interaction.showModal(modal);
    }

    // MODAL NEGAR
    if (interaction.isModalSubmit() && interaction.customId.startsWith("farme_modal_negar:")) {
      await interaction.deferReply({ ephemeral: true });

      if (!isStaff(member, cfg)) {
        return interaction.editReply("❌ Apenas **00** ou **Gerente** pode negar.");
      }

      const requestId = interaction.customId.split(":")[1];
      const req = await getRequestByRequestId(interaction.guild.id, requestId);
      if (!req) return interaction.editReply("❌ Pedido não encontrado.");

      if (req.userId === interaction.user.id) {
        return interaction.editReply("❌ Você não pode **negar seu próprio farme**.");
      }

      if (req.status !== "pending") {
        return interaction.editReply({
          content: "⚠️ Esse pedido já foi avaliado.",
          components: [staffPanelButtons(requestId, is00(member, cfg))],
        });
      }

      const reason = interaction.fields.getTextInputValue("deny_reason")?.trim();
      if (!reason) return interaction.editReply("❌ Motivo vazio.");

      await pool.query(
        `UPDATE farme_requests
         SET status='denied',
             "decidedAt"=NOW(),
             "decidedById"=$1,
             "decidedByTag"=$2,
             "denyReason"=$3
         WHERE "guildId"=$4 AND "requestId"=$5`,
        [interaction.user.id, interaction.user.tag, reason, interaction.guild.id, requestId]
      );

      const embed = makeRequestEmbed({
        userTag: req.userTag,
        userId: req.userId,
        itemLabel: req.itemLabel,
        quantidade: req.quantidade,
        status: "🔴 Negado",
        approverTag: interaction.user.tag,
        reason,
        adjustedInfo: req.adjustedNote || null,
      }).setImage(req.printUrl);

      const channel = await interaction.guild.channels.fetch(req.channelId).catch(() => null);
      if (channel && channel.isTextBased()) {
        const msg = await channel.messages.fetch(req.messageId).catch(() => null);
        if (msg) {
          await msg
            .edit({
              content: `📣 Pedido negado por **${interaction.user.tag}**.`,
              embeds: [embed],
              components: [publicButtons({ disabled: true })],
            })
            .catch(() => null);
        }
      }

      await safeNotifyDM(
        req.userId,
        `❌ Seu farme foi **NEGADO**.\nItem: **${req.itemLabel}**\nQuantidade: **${req.quantidade}**\nNegado por: **${interaction.user.tag}**\nMotivo: **${reason}**`
      );

      await sendLog(
        interaction.guild,
        `🔴 Negado | Membro: <@${req.userId}> | Por: <@${interaction.user.id}>\nItem: **${req.itemLabel}** | Quantidade: **${req.quantidade}**\nMotivo: **${reason}**`,
        embed
      );

      return interaction.editReply({
        content: "✅ Negado com motivo. Painel staff:",
        components: [staffPanelButtons(requestId, is00(member, cfg))],
      });
    }

    // PAINEL STAFF
    if (interaction.isButton() && interaction.customId.startsWith("farme_staff_")) {
      const [action, requestId] = interaction.customId.split(":");
      const req = await getRequestByRequestId(interaction.guild.id, requestId);

      if (!req) return interaction.reply({ content: "❌ Pedido não encontrado.", ephemeral: true });
      if (!isStaff(member, cfg)) return interaction.reply({ content: "❌ Apenas staff.", ephemeral: true });

      if (action === "farme_staff_ajustar") {
        if (!is00(member, cfg)) {
          return interaction.reply({ content: "❌ Apenas o **00** pode ajustar valores.", ephemeral: true });
        }
        if (req.status === "pending") {
          return interaction.reply({ content: "❌ Ajuste só depois de aprovar/negar.", ephemeral: true });
        }

        const modal = new ModalBuilder().setCustomId(`farme_modal_ajustar:${requestId}`).setTitle("Ajustar Farme (00)");

        const opInput = new TextInputBuilder()
          .setCustomId("op")
          .setLabel('Operação: "+", "-", ou "set"')
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(4);

        const valInput = new TextInputBuilder()
          .setCustomId("val")
          .setLabel("Valor (ex: 40)")
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(10);

        const noteInput = new TextInputBuilder()
          .setCustomId("note")
          .setLabel("Observação (opcional)")
          .setStyle(TextInputStyle.Paragraph)
          .setRequired(false)
          .setMaxLength(200);

        modal.addComponents(
          new ActionRowBuilder().addComponents(opInput),
          new ActionRowBuilder().addComponents(valInput),
          new ActionRowBuilder().addComponents(noteInput)
        );

        return interaction.showModal(modal);
      }

      await interaction.deferReply({ ephemeral: true });

      if (action === "farme_staff_fechar") {
        if (req.status === "pending") return interaction.editReply("❌ Você precisa aprovar/negar antes de fechar.");
        if (req.status === "closed") return interaction.editReply("⚠️ Já está fechado. Use Encerrar.");

        const channel = await interaction.guild.channels.fetch(req.channelId).catch(() => null);
        if (!channel) return interaction.editReply("❌ Canal não encontrado.");

        await channel.permissionOverwrites.edit(req.userId, { ViewChannel: false }).catch(() => null);

        if (cfg.CLOSED_CATEGORY_ID) {
          await channel.setParent(cfg.CLOSED_CATEGORY_ID).catch(() => null);
        }

        await channel.setName(`fechado-${channel.name}`.slice(0, 90)).catch(() => null);
        await channel.send(`🔒 Canal fechado por <@${interaction.user.id}>. (Agora só staff vê.)`).catch(() => null);

        await pool.query(
          `UPDATE farme_requests
           SET status='closed',
               "closedAt"=NOW(),
               "closedById"=$1,
               "closedByTag"=$2
           WHERE "guildId"=$3 AND "requestId"=$4`,
          [interaction.user.id, interaction.user.tag, interaction.guild.id, requestId]
        );

        return interaction.editReply("✅ Canal fechado (sumiu pro membro). Agora você pode **Encerrar**.");
      }

      if (action === "farme_staff_encerrar") {
        if (req.status !== "closed") return interaction.editReply("❌ Você precisa Fechar antes de Encerrar.");

        const channel = await interaction.guild.channels.fetch(req.channelId).catch(() => null);
        if (!channel) {
          await pool.query(`DELETE FROM farme_requests WHERE "guildId"=$1 AND "requestId"=$2`, [
            interaction.guild.id,
            requestId,
          ]);
          return interaction.editReply("⚠️ Canal já não existe. Limpei do histórico.");
        }

        await pool.query(`DELETE FROM farme_requests WHERE "guildId"=$1 AND "requestId"=$2`, [
          interaction.guild.id,
          requestId,
        ]);

        await interaction.editReply("🗑️ Encerrando e deletando o canal...");
        await channel.delete(`Encerrado por ${interaction.user.tag}`).catch(() => null);
        return;
      }

      return interaction.editReply("⚠️ Ação desconhecida.");
    }

    // MODAL AJUSTAR FARME (00)
    if (interaction.isModalSubmit() && interaction.customId.startsWith("farme_modal_ajustar:")) {
      await interaction.deferReply({ ephemeral: true });

      if (!is00(member, cfg)) {
        return interaction.editReply("❌ Apenas o **00** pode ajustar.");
      }

      const requestId = interaction.customId.split(":")[1];
      const req = await getRequestByRequestId(interaction.guild.id, requestId);
      if (!req) return interaction.editReply("❌ Pedido não encontrado.");

      const op = (interaction.fields.getTextInputValue("op") || "").trim().toLowerCase();
      const valStr = (interaction.fields.getTextInputValue("val") || "").trim();
      const note = (interaction.fields.getTextInputValue("note") || "").trim();

      const val = parseInt(valStr, 10);
      if (!["+", "-", "set"].includes(op)) return interaction.editReply('❌ Operação inválida. Use: "+", "-", ou "set".');
      if (!Number.isFinite(val)) return interaction.editReply("❌ Valor inválido. Ex: 40");

      let delta = 0;
      if (op === "+") delta = val;
      if (op === "-") delta = -val;
      if (op === "set") delta = val - Number(req.quantidade || 0);

      const novaQuantidade = Math.max(0, Number(req.quantidade || 0) + delta);

      await pool.query(
        `UPDATE farme_requests
         SET quantidade=$1,
             "adjustedAt"=NOW(),
             "adjustedById"=$2,
             "adjustedByTag"=$3,
             "adjustedDelta"=COALESCE("adjustedDelta",0) + $4,
             "adjustedNote"=$5
         WHERE "guildId"=$6 AND "requestId"=$7`,
        [
          novaQuantidade,
          interaction.user.id,
          interaction.user.tag,
          delta,
          `Op: ${op} ${val} | Delta: ${delta}${note ? ` | Nota: ${note}` : ""}`,
          interaction.guild.id,
          requestId,
        ]
      );

      if (req.status === "approved" || req.status === "closed") {
        await addDailyTotals(interaction.guild.id, req.userId, req.itemValue, delta);
        await addWeeklyTotals(interaction.guild.id, req.userId, req.itemValue, delta);
      }

      const req2 = await getRequestByRequestId(interaction.guild.id, requestId);

      const channel = await interaction.guild.channels.fetch(req.channelId).catch(() => null);
      if (channel && channel.isTextBased()) {
        const msg = await channel.messages.fetch(req.messageId).catch(() => null);
        if (msg) {
          const statusTxt =
            req2.status === "approved"
              ? "🟢 Aprovado"
              : req2.status === "denied"
              ? "🔴 Negado"
              : req2.status === "closed"
              ? "🔒 Fechado"
              : "🟡 Pendente";

          const embed = makeRequestEmbed({
            userTag: req2.userTag,
            userId: req2.userId,
            itemLabel: req2.itemLabel,
            quantidade: req2.quantidade,
            status: statusTxt,
            approverTag: req2.decidedByTag || null,
            reason: req2.denyReason || null,
            adjustedInfo: req2.adjustedNote || null,
          }).setImage(req2.printUrl);

          await msg.edit({ embeds: [embed] }).catch(() => null);
        }
      }

      await safeNotifyDM(
        req2.userId,
        `🛠️ Seu farme foi **AJUSTADO** pelo 00.\nItem: **${req2.itemLabel}**\nNovo valor: **${req2.quantidade}**\nDetalhes: ${req2.adjustedNote || "-"}`
      );

      await updatePanelsAfterChange(interaction.guild, req2.userId);

      return interaction.editReply(
        `✅ Ajustado com sucesso. Delta aplicado: **${delta}**. Novo total do pedido: **${req2.quantidade}**`
      );
    }
  } catch (err) {
    console.error("interactionCreate ERROR:", err);
    if (interaction.isRepliable()) {
      if (interaction.deferred || interaction.replied) {
        await interaction.followUp({ content: "❌ Deu erro. Olha o console do Render.", ephemeral: true }).catch(() => null);
      } else {
        await interaction.reply({ content: "❌ Deu erro. Olha o console do Render.", ephemeral: true }).catch(() => null);
      }
    }
  }
});

// ======================================================
// START
// ======================================================
(async () => {
  try {
    await initDB();

    registerCommands().catch((e) => console.error("registerCommands falhou:", e?.rawError || e));

    const loginTimeout = setTimeout(() => {
      console.error("Login travou por 30s. Reiniciando processo...");
      process.exit(1);
    }, 30000);

    client
      .login(process.env.DISCORD_TOKEN)
      .then(() => clearTimeout(loginTimeout))
      .catch((e) => {
        clearTimeout(loginTimeout);
        console.error("LOGIN ERROR:", e?.rawError || e);
        process.exit(1);
      });
  } catch (err) {
    console.error("ERRO AO INICIAR BOT:", err);
    process.exit(1);
  }
})();