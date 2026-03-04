// ✅✅✅ ARQUIVO COMPLETO (com !ajustar + LOG antes/depois + !historico @membro SÓ 00 + !ajuda ajustado + AVISO CANAL DE DÉBITO + lastSeenAt) ✅✅✅
// OBS: Mantive seu código e apliquei os ajustes pra não ter erro.

const {
  Client,
  GatewayIntentBits,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  EmbedBuilder,
  Partials,
} = require("discord.js");

console.log(
  "🔥 BUILD FINAL (+ ALERTA CANAL DE DÉBITO + lastSeenAt por msg + DM 00:05) 🔥",
  new Date().toISOString()
);

const express = require("express");
const { Pool } = require("pg");

// ==========================
// ✅ CONFIG POR SERVIDOR
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
    // (opcional) DEBIT_ALERT_CHANNEL_ID: "...."
  },

  [GUILD_ID_NOVA_ORDEM]: {
    NAME: "NOVA ORDEM",
    GERENTE_ROLE_ID: "1469111029161136386",
    ROLE_00_ID: "1469111029161136392",
    LOG_CHANNEL_ID: "1478096038114885704",
    ENVIO_FARME_CHANNEL_ID: "1478095912789082284",
    DEBIT_ALERT_CHANNEL_ID: "1478556681070706951", // ✅ canal que você pediu
  },
};

function getCfg(guildId) {
  return CONFIGS[guildId] || null;
}

const LOGO_URL = process.env.LOGO_URL || null;
const META_DIARIA = 100;

// ⏰ Fechamento diário (00:05)
const DAILY_AUDIT_HOUR = 0;
const DAILY_AUDIT_MIN = 5;

// ==========================
// 🌐 WEB (Render healthcheck)
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

function getThumb(interactionOrClient) {
  const avatar =
    interactionOrClient?.user?.displayAvatarURL?.() ||
    interactionOrClient?.client?.user?.displayAvatarURL?.() ||
    null;
  return LOGO_URL || avatar || null;
}

async function tableExists(tableName) {
  const { rows } = await pool.query(`SELECT to_regclass($1) AS reg`, [
    `public.${tableName}`,
  ]);
  return !!rows[0]?.reg;
}

async function getUsuariosIdColumnInfo() {
  const { rows } = await pool.query(`
    SELECT
      column_name,
      data_type,
      is_nullable,
      column_default,
      is_identity,
      identity_generation
    FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name='usuarios'
      AND column_name='id'
    LIMIT 1;
  `);
  return rows[0] || null;
}

async function ensureUniqueIndexUsuarios() {
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS usuarios_guild_user_unique
    ON usuarios ("guildId","userId");
  `);
}

async function ensureHistoricoIndexes() {
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_msgId ON historico ("msgId");`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_dia ON historico ("dia");`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_created_at ON historico (created_at);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_guild_user_dia ON historico ("guildId","userId","dia");`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_guild_user_created ON historico ("guildId","userId",created_at DESC);`);
}

// ==========================
// 🧱 INIT DB (MIGRAÇÃO)
// ==========================
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id BIGSERIAL PRIMARY KEY,
      "guildId" TEXT NOT NULL,
      "userId"  TEXT NOT NULL,
      "ultimoDia" TEXT,
      "papelHoje" INTEGER DEFAULT 0,
      "sementesHoje" INTEGER DEFAULT 0,
      "papelCarry" INTEGER DEFAULT 0,
      "sementesCarry" INTEGER DEFAULT 0,
      "papelDebt" INTEGER DEFAULT 0,
      "sementesDebt" INTEGER DEFAULT 0,
      "lastSeenAt" TIMESTAMPTZ
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

  await ensureUniqueIndexUsuarios();

  // ✅ garante coluna lastSeenAt em quem já tinha tabela antiga
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "lastSeenAt" TIMESTAMPTZ;`);

  // Migração/reparo usuarios.id (caso antigo esteja TEXT ou sem default)
  if (await tableExists("usuarios")) {
    const info = await getUsuariosIdColumnInfo();
    if (info) {
      const dataType = (info.data_type || "").toLowerCase();
      const isIdentity = info.is_identity === "YES";
      const hasDefault = !!info.column_default;

      console.log("ℹ️ usuarios.id:", {
        dataType,
        isNullable: info.is_nullable,
        isIdentity,
        hasDefault,
        columnDefault: info.column_default,
      });

      const isNumeric = ["bigint", "integer", "smallint"].includes(dataType);

      if (isNumeric && !isIdentity && !hasDefault) {
        console.log("⚠️ usuarios.id numérico sem DEFAULT/IDENTITY — corrigindo...");
        try {
          await pool.query(
            `ALTER TABLE public.usuarios ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;`
          );
          console.log("✅ usuarios.id virou IDENTITY");
        } catch (e) {
          console.log("⚠️ Falhou IDENTITY, usando sequence...", e?.message || e);
          await pool.query(`CREATE SEQUENCE IF NOT EXISTS public.usuarios_id_seq;`);
          await pool.query(
            `ALTER TABLE public.usuarios ALTER COLUMN id SET DEFAULT nextval('public.usuarios_id_seq');`
          );
          await pool.query(`
            SELECT setval(
              'public.usuarios_id_seq',
              COALESCE((SELECT MAX(id) FROM public.usuarios), 0) + 1,
              false
            );
          `);
          console.log("✅ usuarios.id DEFAULT via sequence OK");
        }
      }

      if (!isNumeric) {
        console.log("⚠️ usuarios.id NÃO é numérico (provável TEXT). Migrando para BIGSERIAL...");
        const c = await pool.connect();
        try {
          await c.query("BEGIN");

          await c.query(`ALTER TABLE public.usuarios ADD COLUMN IF NOT EXISTS id_new BIGSERIAL;`);

          const pk = await c.query(`
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema='public'
              AND table_name='usuarios'
              AND constraint_type='PRIMARY KEY'
            LIMIT 1;
          `);

          if (pk.rows[0]?.constraint_name) {
            await c.query(`ALTER TABLE public.usuarios DROP CONSTRAINT ${pk.rows[0].constraint_name};`);
          }

          await c.query(`ALTER TABLE public.usuarios DROP COLUMN id;`);
          await c.query(`ALTER TABLE public.usuarios RENAME COLUMN id_new TO id;`);
          await c.query(`ALTER TABLE public.usuarios ADD PRIMARY KEY (id);`);

          await c.query("COMMIT");
          console.log("✅ Migração usuarios.id (TEXT -> BIGSERIAL) concluída");
        } catch (e) {
          await c.query("ROLLBACK");
          console.error("❌ Falha migrando usuarios.id:", e?.stack || e);
          throw e;
        } finally {
          c.release();
        }
      }
    }
  }

  await ensureHistoricoIndexes();

  // normaliza nulls
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
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.DirectMessages,
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

function getDebitAlertChannel(guild) {
  const cfg = getCfg(guild.id);
  if (!cfg?.DEBIT_ALERT_CHANNEL_ID) return null;
  return guild.channels.cache.get(cfg.DEBIT_ALERT_CHANNEL_ID) || null;
}

function getHighestRoleName(member) {
  if (!member) return "-";
  const roles = member.roles.cache
    .filter((r) => r.id !== member.guild.id) // remove @everyone
    .sort((a, b) => b.position - a.position);

  return roles.first()?.name || "-";
}

function isEnvioChannel(message) {
  const cfg = getCfg(message.guild.id);
  if (!cfg) return false;
  return message.channel.id === cfg.ENVIO_FARME_CHANNEL_ID;
}

async function ensureUser(guildId, userId) {
  guildId = String(guildId ?? "");
  userId = String(userId ?? "");
  if (!guildId || !userId) return null;

  let res = await pool.query(`SELECT * FROM usuarios WHERE "guildId"=$1 AND "userId"=$2`, [guildId, userId]);
  if (res.rows[0]) return res.rows[0];

  const hoje = todayKey();

  await pool.query(
    `INSERT INTO usuarios ("guildId","userId","ultimoDia","papelHoje","sementesHoje","papelCarry","sementesCarry","papelDebt","sementesDebt","lastSeenAt")
     VALUES ($1,$2,$3,0,0,0,0,0,0,NULL)
     ON CONFLICT ("guildId","userId") DO NOTHING`,
    [guildId, userId, hoje]
  );

  res = await pool.query(`SELECT * FROM usuarios WHERE "guildId"=$1 AND "userId"=$2`, [guildId, userId]);
  return res.rows[0] || null;
}

async function rollToToday(guildId, userId) {
  const hoje = todayKey();
  const u = await ensureUser(guildId, userId);
  if (!u) return null;
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

  const r = await pool.query(`SELECT * FROM usuarios WHERE "guildId"=$1 AND "userId"=$2`, [guildId, userId]);
  return r.rows[0] || null;
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
  return { papel: rows[0]?.papel_total ?? 0, sementes: rows[0]?.sementes_total ?? 0 };
}

// ==========================
// 📊 TABELA / RELATÓRIO / NÃO FARMOU
// ==========================
async function getTabelaResumoDia(guildId, diaStr) {
  const { rows } = await pool.query(
    `
    SELECT
      "userId",
      COALESCE(SUM(CASE WHEN status='APROVADO' AND tipo='papel'   THEN COALESCE(aplicado,0) ELSE 0 END),0)::int AS papel,
      COALESCE(SUM(CASE WHEN status='APROVADO' AND tipo='sementes' THEN COALESCE(aplicado,0) ELSE 0 END),0)::int AS sementes,
      COALESCE(SUM(CASE WHEN status='APROVADO' THEN COALESCE(aplicado,0) ELSE 0 END),0)::int AS total
    FROM historico
    WHERE "guildId"=$1 AND dia=$2 AND status='APROVADO'
    GROUP BY "userId"
    HAVING COALESCE(SUM(CASE WHEN status='APROVADO' THEN COALESCE(aplicado,0) ELSE 0 END),0) > 0
    ORDER BY total DESC, papel DESC, sementes DESC
    `,
    [guildId, diaStr]
  );
  return rows;
}

async function getResumoGeralDia(guildId, diaStr) {
  const { rows } = await pool.query(
    `
    SELECT
      COUNT(DISTINCT "userId")::int AS total_membros,
      COALESCE(SUM(CASE WHEN status='APROVADO' AND tipo='papel' THEN COALESCE(aplicado,0) ELSE 0 END),0)::int AS papel_total,
      COALESCE(SUM(CASE WHEN status='APROVADO' AND tipo='sementes' THEN COALESCE(aplicado,0) ELSE 0 END),0)::int AS sementes_total
    FROM historico
    WHERE "guildId"=$1 AND dia=$2 AND status='APROVADO'
    `,
    [guildId, diaStr]
  );
  return rows[0] || { total_membros: 0, papel_total: 0, sementes_total: 0 };
}

async function getAprovadosUserIdsDia(guildId, diaStr) {
  const { rows } = await pool.query(
    `
    SELECT DISTINCT "userId"
    FROM historico
    WHERE "guildId"=$1 AND dia=$2 AND status='APROVADO'
    `,
    [guildId, diaStr]
  );
  return new Set(rows.map((r) => String(r.userId)));
}

function chunkTextLines(lines, maxLen = 3800) {
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

function resolveRoleFromMessage(message, rawArg) {
  const mentioned = message.mentions?.roles?.first?.();
  if (mentioned) return mentioned;

  const name = (rawArg || "").trim();
  if (!name) return null;

  const role = message.guild.roles.cache.find((r) => r.name.toLowerCase() === name.toLowerCase());
  return role || null;
}

// ==========================
// ✅ APPLY FARM (BOTÃO APROVAR)
// ==========================
async function applyFarm(guildId, userId, tipo, quantidade) {
  const u0 = await rollToToday(guildId, userId);
  if (!u0) throw new Error("Usuário inválido (rollToToday retornou null)");

  let papelHoje = Number(u0.papelHoje || 0);
  let sementesHoje = Number(u0.sementesHoje || 0);
  let papelCarry = Number(u0.papelCarry || 0);
  let sementesCarry = Number(u0.sementesCarry || 0);
  let papelDebt = Number(u0.papelDebt || 0);
  let sementesDebt = Number(u0.sementesDebt || 0);

  let aplicado = 0;
  let carry = 0;

  if (tipo === "papel") {
    const restante = Math.max(0, 100 - papelHoje);
    aplicado = Math.min(quantidade, restante);

    let overflow = Math.max(0, quantidade - aplicado);

    const paga = Math.min(overflow, papelDebt);
    papelDebt -= paga;
    overflow -= paga;

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
  return { user: u1.rows[0], aplicado, carry };
}

// ==========================
// ✏️ EDITAR / DESFAZER (REPLY) — SÓ 00
// ==========================
function calcDebtPaid(quantidade, aplicado, carry) {
  const q = Number(quantidade || 0);
  const a = Number(aplicado || 0);
  const c = Number(carry || 0);
  return Math.max(0, (q - a) - c);
}

async function getHistoricoAprovadoByMsgId(guildId, msgId) {
  const { rows } = await pool.query(
    `SELECT *
     FROM historico
     WHERE "guildId"=$1 AND "msgId"=$2 AND status='APROVADO'
     ORDER BY id DESC
     LIMIT 1`,
    [guildId, msgId]
  );
  return rows[0] || null;
}

async function getAprovadosUserTipoDiaForUpdate(dbClient, guildId, userId, tipo, dia) {
  const { rows } = await dbClient.query(
    `SELECT id, "msgId", quantidade, aplicado, carry, created_at
     FROM historico
     WHERE "guildId"=$1 AND "userId"=$2 AND tipo=$3 AND dia=$4 AND status='APROVADO'
     ORDER BY created_at ASC, id ASC
     FOR UPDATE`,
    [guildId, userId, tipo, dia]
  );
  return rows;
}

async function recalcTipoDia(dbClient, guildId, userId, tipo, dia, editedRowId, newQuantidade) {
  const userRes = await dbClient.query(
    `SELECT *
     FROM usuarios
     WHERE "guildId"=$1 AND "userId"=$2
     FOR UPDATE`,
    [guildId, userId]
  );
  const u = userRes.rows[0];
  if (!u) throw new Error("Usuário não encontrado em usuarios");

  const rows = await getAprovadosUserTipoDiaForUpdate(dbClient, guildId, userId, tipo, dia);
  if (!rows.length) return;

  let papelHoje = Number(u.papelHoje || 0);
  let sementesHoje = Number(u.sementesHoje || 0);
  let papelCarry = Number(u.papelCarry || 0);
  let sementesCarry = Number(u.sementesCarry || 0);
  let papelDebt = Number(u.papelDebt || 0);
  let sementesDebt = Number(u.sementesDebt || 0);

  // reverte todas as aplicações do tipo no dia
  for (const r of rows) {
    const debtPaid = calcDebtPaid(r.quantidade, r.aplicado, r.carry);

    if (tipo === "papel") {
      papelHoje = Math.max(0, papelHoje - Number(r.aplicado || 0));
      papelCarry = Math.max(0, papelCarry - Number(r.carry || 0));
      papelDebt = Math.max(0, papelDebt + debtPaid);
    } else {
      sementesHoje = Math.max(0, sementesHoje - Number(r.aplicado || 0));
      sementesCarry = Math.max(0, sementesCarry - Number(r.carry || 0));
      sementesDebt = Math.max(0, sementesDebt + debtPaid);
    }
  }

  // atualiza quantidade do registro editado
  await dbClient.query(`UPDATE historico SET quantidade=$1 WHERE id=$2`, [Number(newQuantidade), editedRowId]);

  // reaplica em ordem cronológica
  const rows2 = await getAprovadosUserTipoDiaForUpdate(dbClient, guildId, userId, tipo, dia);

  for (const r of rows2) {
    const q = Number(r.quantidade || 0);
    let aplicado = 0;
    let carry = 0;

    if (tipo === "papel") {
      const restante = Math.max(0, 100 - papelHoje);
      aplicado = Math.min(q, restante);

      let overflow = Math.max(0, q - aplicado);
      const paga = Math.min(overflow, papelDebt);
      papelDebt -= paga;
      overflow -= paga;

      carry = overflow;

      papelHoje += aplicado;
      papelCarry += carry;
    } else {
      const restante = Math.max(0, 100 - sementesHoje);
      aplicado = Math.min(q, restante);

      let overflow = Math.max(0, q - aplicado);
      const paga = Math.min(overflow, sementesDebt);
      sementesDebt -= paga;
      overflow -= paga;

      carry = overflow;

      sementesHoje += aplicado;
      sementesCarry += carry;
    }

    await dbClient.query(`UPDATE historico SET aplicado=$1, carry=$2 WHERE id=$3`, [aplicado, carry, r.id]);
  }

  await dbClient.query(
    `UPDATE usuarios
     SET "papelHoje"=$1, "sementesHoje"=$2, "papelCarry"=$3, "sementesCarry"=$4, "papelDebt"=$5, "sementesDebt"=$6
     WHERE "guildId"=$7 AND "userId"=$8`,
    [papelHoje, sementesHoje, papelCarry, sementesCarry, papelDebt, sementesDebt, guildId, userId]
  );
}

// ==========================
// ✅ AJUSTAR (SÓ 00) — sem reply
// ==========================
async function applyAdjustDirect(guildId, userId, tipo, delta) {
  const u0 = await rollToToday(guildId, userId);
  if (!u0) throw new Error("Usuário inválido");

  let papelHoje = Number(u0.papelHoje || 0);
  let sementesHoje = Number(u0.sementesHoje || 0);
  let papelCarry = Number(u0.papelCarry || 0);
  let sementesCarry = Number(u0.sementesCarry || 0);
  let papelDebt = Number(u0.papelDebt || 0);
  let sementesDebt = Number(u0.sementesDebt || 0);

  const d = Number(delta || 0);
  if (!d) return { before: u0, after: u0 };

  if (tipo === "papel") {
    if (d > 0) {
      const restante = Math.max(0, 100 - papelHoje);
      const aplicado = Math.min(d, restante);
      const overflow = Math.max(0, d - aplicado);
      papelHoje += aplicado;
      papelCarry += overflow;
    } else {
      let remove = Math.abs(d);
      const takeCarry = Math.min(remove, papelCarry);
      papelCarry -= takeCarry;
      remove -= takeCarry;

      const takeHoje = Math.min(remove, papelHoje);
      papelHoje -= takeHoje;
      remove -= takeHoje;
    }

    await pool.query(
      `UPDATE usuarios SET "papelHoje"=$1,"papelCarry"=$2,"papelDebt"=$3 WHERE "guildId"=$4 AND "userId"=$5`,
      [papelHoje, papelCarry, papelDebt, guildId, userId]
    );
  } else if (tipo === "sementes") {
    if (d > 0) {
      const restante = Math.max(0, 100 - sementesHoje);
      const aplicado = Math.min(d, restante);
      const overflow = Math.max(0, d - aplicado);
      sementesHoje += aplicado;
      sementesCarry += overflow;
    } else {
      let remove = Math.abs(d);
      const takeCarry = Math.min(remove, sementesCarry);
      sementesCarry -= takeCarry;
      remove -= takeCarry;

      const takeHoje = Math.min(remove, sementesHoje);
      sementesHoje -= takeHoje;
      remove -= takeHoje;
    }

    await pool.query(
      `UPDATE usuarios SET "sementesHoje"=$1,"sementesCarry"=$2,"sementesDebt"=$3 WHERE "guildId"=$4 AND "userId"=$5`,
      [sementesHoje, sementesCarry, sementesDebt, guildId, userId]
    );
  } else {
    throw new Error("tipo inválido");
  }

  const { rows } = await pool.query(`SELECT * FROM usuarios WHERE "guildId"=$1 AND "userId"=$2`, [guildId, userId]);
  return { before: u0, after: rows[0] || u0 };
}

// ==========================
// ✅ HISTÓRICO
// ==========================
function statusEmoji(status) {
  const s = String(status || "").toUpperCase();
  if (s === "APROVADO") return "✅";
  if (s === "NEGADO") return "❌";
  if (s === "AJUSTE") return "👑";
  if (s === "EDITADO") return "✏️";
  if (s === "DESFEITO") return "🔁";
  return "📌";
}

async function getHistoricoUser(guildId, userId, limit = 30, opts = {}) {
  const lim = Math.max(1, Math.min(100, Number(limit || 30)));
  const dia = opts?.dia || null;
  const tipo = opts?.tipo || null;

  const params = [guildId, userId];
  let where = `WHERE "guildId"=$1 AND "userId"=$2`;
  let p = 3;

  if (dia) {
    where += ` AND dia=$${p++}`;
    params.push(dia);
  }
  if (tipo) {
    where += ` AND tipo=$${p++}`;
    params.push(tipo);
  }

  params.push(lim);

  const { rows } = await pool.query(
    `
    SELECT tipo, quantidade, status, data, dia, "msgId", "gerenteId", aplicado, carry, created_at
    FROM historico
    ${where}
    ORDER BY created_at DESC, id DESC
    LIMIT $${p}
    `,
    params
  );

  return rows;
}

// ==========================
// ✅ FECHAMENTO DIÁRIO + DM 00:05 + AVISO NO CANAL
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
  if (!(now.getHours() === DAILY_AUDIT_HOUR && now.getMinutes() === DAILY_AUDIT_MIN)) return;

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
      const totals = await getTotaisDia(guild.id, u.userId, diaAuditado);
      const faltouP = Math.max(0, META_DIARIA - totals.papel);
      const faltouS = Math.max(0, META_DIARIA - totals.sementes);

      if (faltouP > 0 || faltouS > 0) {
        await pool.query(
          `UPDATE usuarios
           SET "papelDebt" = COALESCE("papelDebt",0) + $1,
               "sementesDebt" = COALESCE("sementesDebt",0) + $2
           WHERE "guildId"=$3 AND "userId"=$4`,
          [faltouP, faltouS, guild.id, u.userId]
        );

        faltaram.push({
          userId: u.userId,
          papel_total: totals.papel,
          sementes_total: totals.sementes,
          faltouP,
          faltouS,
        });
      }
    }

    // 🔔 DM automática + 📢 aviso no canal (se configurado)
    for (const r of faltaram) {
      try {
        const member = await guild.members.fetch(r.userId).catch(() => null);
        if (!member || member.user.bot) continue;

        const totalFaltou = Number(r.faltouP || 0) + Number(r.faltouS || 0);

        const embedDM = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle("⚠️ Meta diária não atingida")
          .setDescription(
            `🏷️ Servidor: **${cfg.NAME}**\n` +
              `📅 Dia analisado: **${diaAuditado}**\n\n` +
              `📄 Papel: **${r.papel_total}/${META_DIARIA}**\n` +
              `🌱 Sementes: **${r.sementes_total}/${META_DIARIA}**\n\n` +
              `❌ Faltou:\n` +
              `• Papel: **${r.faltouP}**\n` +
              `• Sementes: **${r.faltouS}**\n\n` +
              `📦 Total faltante: **${totalFaltou}**\n\n` +
              `Essa quantidade foi adicionada ao seu **Farme atrasado**.`
          )
          .setTimestamp();

        const thumb = getThumb(client);
        if (thumb) embedDM.setThumbnail(thumb);

        // ✅ manda DM
        await member.send({ embeds: [embedDM] }).catch(() => null);

        // ✅ dados atuais do banco (debt total + lastSeenAt)
        const uRes = await pool.query(
          `SELECT "papelDebt","sementesDebt","lastSeenAt"
           FROM usuarios
           WHERE "guildId"=$1 AND "userId"=$2
           LIMIT 1`,
          [guild.id, member.user.id]
        );
        const uRow = uRes.rows[0] || {};
        const papelDebtNow = Number(uRow.papelDebt || 0);
        const sementesDebtNow = Number(uRow.sementesDebt || 0);
        const totalDebtNow = papelDebtNow + sementesDebtNow;

        const lastSeenTxt = uRow.lastSeenAt
          ? `<t:${Math.floor(new Date(uRow.lastSeenAt).getTime() / 1000)}:R>`
          : "Sem registro";

        const cargoAtual = getHighestRoleName(member);

        // ✅ manda aviso no canal configurado (NOVA ORDEM)
        const avisoChannel = getDebitAlertChannel(guild);
        if (avisoChannel) {
          const embedAviso = new EmbedBuilder()
            .setColor("#ff4d4d")
            .setTitle("🚨 Membro ficou devendo meta")
            .setDescription(
              `👤 Membro: **${member.user.tag}**\n` +
                `🆔 ID: **${member.user.id}**\n` +
                `🎭 Cargo atual: **${cargoAtual}**\n` +
                `🕒 Último visto (pelo bot): **${lastSeenTxt}**\n\n` +
                `📅 Dia auditado: **${diaAuditado}**\n\n` +
                `❌ Faltou no dia:\n` +
                `• Papel: **${r.faltouP}**\n` +
                `• Sementes: **${r.faltouS}**\n` +
                `📦 Total faltante: **${totalFaltou}**\n\n` +
                `📌 Dívida acumulada agora:\n` +
                `• Papel: **${papelDebtNow}**\n` +
                `• Sementes: **${sementesDebtNow}**\n` +
                `📦 Total acumulado: **${totalDebtNow}**`
            )
            .setTimestamp();

          if (thumb) embedAviso.setThumbnail(thumb);
          await avisoChannel.send({ embeds: [embedAviso] }).catch(() => null);
        }
      } catch (e) {}
    }

    const logChannel = getLogChannel(guild);
    if (!logChannel) {
      await setConfig(lastDoneKey, today);
      continue;
    }

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
        const pTxt =
          r.faltouP > 0 ? `❌ ${r.papel_total}/${META_DIARIA} (faltou ${r.faltouP})` : `✅ ${r.papel_total}/${META_DIARIA}`;
        const sTxt =
          r.faltouS > 0 ? `❌ ${r.sementes_total}/${META_DIARIA} (faltou ${r.faltouS})` : `✅ ${r.sementes_total}/${META_DIARIA}`;
        return `• <@${r.userId}> — 📄 ${pTxt} | 🌱 ${sTxt}`;
      });

      for (const chunk of chunkLines(lines)) {
        const embed = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle(`⚠️ Fechamento diário: NÃO bateu meta — ${cfg.NAME}`)
          .setDescription(
            `📅 Dia auditado: **${diaAuditado}**\n🎯 Meta diária: **${META_DIARIA}**\n📌 A diferença foi somada no **Farme atrasado**.\n\n${chunk}`
          )
          .setTimestamp();
        if (thumb) embed.setThumbnail(thumb);
        await logChannel.send({ embeds: [embed] }).catch(() => null);
      }
    }

    await setConfig(lastDoneKey, today);
  }
}

async function startDailyAuditLoop() {
  setInterval(() => runDailyAuditOnce().catch((e) => console.error("Erro runDailyAuditOnce:", e)), 30_000);
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

    // ✅ garante usuário e marca "último visto"
    await ensureUser(message.guild.id, message.author.id);
    await pool.query(
      `UPDATE usuarios SET "lastSeenAt"=NOW()
       WHERE "guildId"=$1 AND "userId"=$2`,
      [message.guild.id, message.author.id]
    );

    const member = message.member || (await message.guild.members.fetch(message.author.id));
    const isGerente = member.roles.cache.has(cfg.GERENTE_ROLE_ID);
    const is00 = member.roles.cache.has(cfg.ROLE_00_ID);

    const content = (message.content || "").trim();
    const lower = content.toLowerCase();

    // ==========================
    // 📖 !ajuda
    // ==========================
    if (lower === "!ajuda") {
      let desc = "";

      desc += `👤 **Membro**\n`;
      desc += `\`!status\` — ver seu status do dia\n`;
      desc += `\`!farme papel X\` / \`!farme sementes X\` — enviar farme com print (no canal correto)\n`;

      if (isGerente || is00) {
        desc += `\n🛡️ **Gerente / 00**\n`;
        desc += `\`!tabela\` / \`!tabela ontem\` — ranking de aprovados\n`;
        desc += `\`!relatorio\` — relatório geral do dia\n`;
        desc += `\`!naofarmou\` / \`!naofarmou @cargo\` — lista quem não teve farme aprovado\n`;
      }

      if (is00) {
        desc += `\n👑 **Somente 00**\n`;
        desc += `\`!historico @membro\` — histórico do membro (últimos registros)\n`;
        desc += `\n*(reply na mensagem aprovada do bot)*\n`;
        desc += `\`!editar 10\` — corrige a quantidade aprovada\n`;
        desc += `\`!desfazer\` — volta o aprovado pra 0\n`;
        desc += `\n*(sem reply — ajuste administrativo direto no saldo)*\n`;
        desc += `\`!ajustar @membro papel +80\`\n`;
        desc += `\`!ajustar @membro papel -120\`\n`;
        desc += `\`!ajustar @membro sementes +50\`\n`;
        desc += `\`!ajustar @membro sementes -200\`\n`;
      }

      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle(`📖 Comandos disponíveis — ${cfg.NAME}`)
        .setDescription(desc)
        .setTimestamp();

      const thumb = getThumb(client);
      if (thumb) embed.setThumbnail(thumb);

      return message.reply({ embeds: [embed] });
    }

    // ==========================
    // 🧾 !historico (SÓ 00)
    // ==========================
    if (lower.startsWith("!historico")) {
      if (!is00) return message.reply("❌ Só o **00** pode usar esse comando.");

      const target = message.mentions.users.first();
      if (!target) return message.reply("⚠️ Use: `!historico @membro` (opcional: `hoje|ontem` `papel|sementes` `50`)");

      const parts = content.split(/\s+/);

      let diaOpt = null;
      let tipoOpt = null;
      let limit = 30;

      for (let i = 2; i < parts.length; i++) {
        const p = (parts[i] || "").toLowerCase();

        if (p === "hoje") diaOpt = todayKey();
        else if (p === "ontem") diaOpt = yesterdayKey();
        else if (p === "papel" || p === "sementes") tipoOpt = p;
        else {
          const n = parseInt(p, 10);
          if (!isNaN(n)) limit = n;
        }
      }

      const rows = await getHistoricoUser(message.guild.id, target.id, limit, { dia: diaOpt, tipo: tipoOpt });

      if (!rows.length) {
        return message.reply(`📌 Não achei histórico para <@${target.id}> nesse filtro.`);
      }

      const lines = rows.map((r, i) => {
        const emo = statusEmoji(r.status);
        const tipo = (r.tipo || "").toUpperCase();
        const q = Number(r.quantidade ?? 0);
        const aplicado = r.aplicado == null ? "-" : Number(r.aplicado);
        const carry = r.carry == null ? "-" : Number(r.carry);
        const staff = r.gerenteId ? `<@${r.gerenteId}>` : "-";
        const dia = r.dia || "-";
        return `${i + 1}. ${emo} **${r.status}** • **${tipo}** • q=${q} • aplicado=${aplicado} • extra=${carry} • dia=${dia} • por=${staff}`;
      });

      const chunks = chunkTextLines(lines, 3800);
      const thumb = getThumb(client);

      const filtroTxt =
        `👤 Usuário: <@${target.id}>\n` +
        `🔎 Filtro: ${diaOpt ? `dia=${diaOpt}` : "dia=TODOS"} | ${tipoOpt ? `tipo=${tipoOpt}` : "tipo=TODOS"}\n\n`;

      for (let idx = 0; idx < chunks.length; idx++) {
        const embed = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle(`🧾 Histórico — ${cfg.NAME}`)
          .setDescription(`${filtroTxt}${chunks[idx]}`)
          .setTimestamp();

        if (thumb) embed.setThumbnail(thumb);

        if (idx === 0) await message.reply({ embeds: [embed] });
        else await message.channel.send({ embeds: [embed] });
      }
      return;
    }

    // ==========================
    // 👑 !ajustar (SÓ 00)
    // ==========================
    if (lower.startsWith("!ajustar")) {
      if (!is00) return message.reply("❌ Só o **00** pode usar esse comando.");

      const target = message.mentions.users.first();
      if (!target) {
        return message.reply("⚠️ Use assim: `!ajustar @membro papel +80` ou `!ajustar @membro sementes -120`");
      }

      const parts = content.split(/\s+/);
      const tipo = (parts[2] || "").toLowerCase();
      const deltaStr = parts[3] || "";

      if (!["papel", "sementes"].includes(tipo)) {
        return message.reply("❌ Tipo inválido. Use `papel` ou `sementes`.\nEx: `!ajustar @membro papel +80`");
      }
      if (!deltaStr) {
        return message.reply("❌ Falta o valor. Ex: `!ajustar @membro papel -120`");
      }

      const delta = parseInt(deltaStr, 10);
      if (isNaN(delta) || delta === 0) {
        return message.reply("❌ Valor inválido. Use número inteiro diferente de 0.\nEx: `+80` ou `-120`");
      }

      await ensureUser(message.guild.id, target.id);

      const { before, after } = await applyAdjustDirect(message.guild.id, target.id, tipo, delta);

      await insertHistorico({
        guildId: message.guild.id,
        userId: target.id,
        tipo,
        quantidade: delta,
        status: "AJUSTE",
        msgId: message.id,
        gerenteId: message.author.id,
        aplicado: null,
        carry: null,
        dia: todayKey(),
      });

      const bP = Number(before.papelHoje || 0), bPC = Number(before.papelCarry || 0);
      const bS = Number(before.sementesHoje || 0), bSC = Number(before.sementesCarry || 0);
      const aP = Number(after.papelHoje || 0), aPC = Number(after.papelCarry || 0);
      const aS = Number(after.sementesHoje || 0), aSC = Number(after.sementesCarry || 0);

      const deltaTxt = delta > 0 ? `+${delta}` : `${delta}`;

      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle("👑 Ajuste aplicado (00)")
        .setDescription(
          `👤 Usuário: <@${target.id}>\n` +
          `🧾 Tipo: **${tipo.toUpperCase()}**\n` +
          `🔧 Ajuste: **${deltaTxt}**\n\n` +
          `🔎 **ANTES**\n` +
          `📄 Papel: **${bP}/100** (extra: ${bPC})\n` +
          `🌱 Sementes: **${bS}/100** (extra: ${bSC})\n\n` +
          `✅ **DEPOIS**\n` +
          `📄 Papel: **${aP}/100** (extra: ${aPC})\n` +
          `🌱 Sementes: **${aS}/100** (extra: ${aSC})\n\n` +
          `🛡️ Ajustado por: <@${message.author.id}>`
        )
        .setTimestamp();

      const thumb = getThumb(client);
      if (thumb) embed.setThumbnail(thumb);

      await message.reply({ embeds: [embed] });

      const logChannel = getLogChannel(message.guild);
      if (logChannel) {
        const logEmbed = new EmbedBuilder()
          .setColor("#ff4d4d")
          .setTitle(`🚨 AJUSTE ADMINISTRATIVO — ${cfg.NAME}`)
          .setDescription(
            `👤 Usuário: <@${target.id}>\n` +
            `📦 Tipo: **${tipo.toUpperCase()}**\n\n` +
            `🔎 **ANTES**\n` +
            `📄 Papel: ${bP}/100 (extra: ${bPC})\n` +
            `🌱 Sementes: ${bS}/100 (extra: ${bSC})\n\n` +
            `✏️ **AJUSTE**\n` +
            `${deltaTxt}\n\n` +
            `✅ **DEPOIS**\n` +
            `📄 Papel: ${aP}/100 (extra: ${aPC})\n` +
            `🌱 Sementes: ${aS}/100 (extra: ${aSC})\n\n` +
            `🛡️ Por: <@${message.author.id}>`
          )
          .setTimestamp();

        if (thumb) logEmbed.setThumbnail(thumb);
        await logChannel.send({ embeds: [logEmbed] }).catch(() => null);
      }

      return;
    }

    // ==========================
    // ✏️ !editar (SÓ 00) — reply no aprovado do BOT
    // ==========================
    if (lower.startsWith("!editar")) {
      if (!is00) return message.reply("❌ Só o **00** pode usar esse comando.");

      const refMsgId = message.reference?.messageId || null;
      if (!refMsgId) return message.reply("⚠️ Use **reply** na mensagem aprovada do bot e mande `!editar 10`.");

      const parts = content.split(/\s+/);
      let tipoArg = null;
      let qtyArg = null;

      if (parts.length === 2) {
        qtyArg = parts[1];
      } else if (parts.length >= 3) {
        tipoArg = (parts[1] || "").toLowerCase();
        qtyArg = parts[2];
      }

      const newQty = parseInt(qtyArg, 10);
      if (isNaN(newQty) || newQty <= 0) return message.reply("❌ Quantidade inválida. Ex: `!editar 10`");

      const h = await getHistoricoAprovadoByMsgId(message.guild.id, refMsgId);
      if (!h) return message.reply("⚠️ Não achei farme **APROVADO** pra essa mensagem.");

      const dia = todayKey();
      if (String(h.dia) !== String(dia)) {
        return message.reply("⚠️ Por segurança, só dá pra editar farme **do dia de hoje**.");
      }

      if (tipoArg && !["papel", "sementes"].includes(tipoArg)) {
        return message.reply("❌ Tipo inválido. Use `papel` ou `sementes` (ou só `!editar 10`).");
      }
      if (tipoArg && tipoArg !== h.tipo) {
        return message.reply(`⚠️ Esse farme é do tipo **${h.tipo}**. Use: \`!editar ${newQty}\` (ou o tipo correto).`);
      }

      await ensureUser(message.guild.id, h.userId);

      const beforeUser = await rollToToday(message.guild.id, h.userId);

      const dbClient = await pool.connect();
      try {
        await dbClient.query("BEGIN");
        await recalcTipoDia(dbClient, message.guild.id, h.userId, h.tipo, dia, h.id, newQty);
        await dbClient.query("COMMIT");
      } catch (e) {
        await dbClient.query("ROLLBACK");
        console.error("❌ Erro no !editar:", e?.stack || e);
        return message.reply("❌ Deu erro ao editar. Veja os logs do Render.");
      } finally {
        dbClient.release();
      }

      const h2 = await getHistoricoAprovadoByMsgId(message.guild.id, refMsgId);
      const afterUser = await rollToToday(message.guild.id, h.userId);

      await insertHistorico({
        guildId: message.guild.id,
        userId: h.userId,
        tipo: h.tipo,
        quantidade: newQty,
        status: "EDITADO",
        msgId: message.id,
        gerenteId: message.author.id,
        aplicado: h2?.aplicado ?? null,
        carry: h2?.carry ?? null,
        dia: todayKey(),
      });

      try {
        const refMsg = await message.channel.messages.fetch(refMsgId);
        if (refMsg?.author?.id === client.user.id) {
          await refMsg
            .edit({
              content:
                `✅ **Aprovado (EDITADO)**\n` +
                `📦 ${h.tipo.toUpperCase()} • ${newQty}\n` +
                `➡️ Aplicado hoje: **${h2?.aplicado ?? 0}** | Extra (amanhã): **${h2?.carry ?? 0}**\n\n` +
                `👤 Usuário: <@${h.userId}>\n` +
                `🛡️ Editado por: <@${message.author.id}>`,
              components: [],
            })
            .catch(() => null);
        }
      } catch {}

      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle("✏️ Farme editado com sucesso")
        .setDescription(
          `👤 Usuário: <@${h.userId}>\n` +
          `🧾 Tipo: **${h.tipo.toUpperCase()}**\n` +
          `📦 Nova quantidade: **${newQty}**\n` +
          `➡️ Aplicado hoje: **${h2?.aplicado ?? 0}** | Extra (amanhã): **${h2?.carry ?? 0}**\n` +
          `📅 Dia: **${dia}**`
        )
        .setTimestamp();

      const thumb = getThumb(client);
      if (thumb) embed.setThumbnail(thumb);

      await message.reply({ embeds: [embed] });

      const logChannel = getLogChannel(message.guild);
      if (logChannel) {
        const bP = Number(beforeUser?.papelHoje || 0), bPC = Number(beforeUser?.papelCarry || 0);
        const bS = Number(beforeUser?.sementesHoje || 0), bSC = Number(beforeUser?.sementesCarry || 0);
        const aP = Number(afterUser?.papelHoje || 0), aPC = Number(afterUser?.papelCarry || 0);
        const aS = Number(afterUser?.sementesHoje || 0), aSC = Number(afterUser?.sementesCarry || 0);

        const logEmbed = new EmbedBuilder()
          .setColor("#ffb020")
          .setTitle(`🚨 EDITADO (00) — ${cfg.NAME}`)
          .setDescription(
            `👤 Usuário: <@${h.userId}>\n` +
            `🧾 Tipo: **${h.tipo.toUpperCase()}**\n` +
            `📦 Nova quantidade (aprovada): **${newQty}**\n\n` +
            `🔎 **ANTES**\n` +
            `📄 Papel: ${bP}/100 (extra: ${bPC})\n` +
            `🌱 Sementes: ${bS}/100 (extra: ${bSC})\n\n` +
            `✅ **DEPOIS**\n` +
            `📄 Papel: ${aP}/100 (extra: ${aPC})\n` +
            `🌱 Sementes: ${aS}/100 (extra: ${aSC})\n\n` +
            `🛡️ Por: <@${message.author.id}>`
          )
          .setTimestamp();

        if (thumb) logEmbed.setThumbnail(thumb);
        await logChannel.send({ embeds: [logEmbed] }).catch(() => null);
      }

      return;
    }

    // ==========================
    // 🔁 !desfazer (SÓ 00) — reply no aprovado do BOT
    // ==========================
    if (lower === "!desfazer") {
      if (!is00) return message.reply("❌ Só o **00** pode usar esse comando.");

      const refMsgId = message.reference?.messageId || null;
      if (!refMsgId) return message.reply("⚠️ Use **reply** na mensagem aprovada do bot e mande `!desfazer`.");

      const h = await getHistoricoAprovadoByMsgId(message.guild.id, refMsgId);
      if (!h) return message.reply("⚠️ Não achei farme **APROVADO** pra essa mensagem.");

      const dia = todayKey();
      if (String(h.dia) !== String(dia)) {
        return message.reply("⚠️ Por segurança, só dá pra desfazer farme **do dia de hoje**.");
      }

      await ensureUser(message.guild.id, h.userId);

      const beforeUser = await rollToToday(message.guild.id, h.userId);

      const dbClient = await pool.connect();
      try {
        await dbClient.query("BEGIN");
        await recalcTipoDia(dbClient, message.guild.id, h.userId, h.tipo, dia, h.id, 0);
        await dbClient.query("COMMIT");
      } catch (e) {
        await dbClient.query("ROLLBACK");
        console.error("❌ Erro no !desfazer:", e?.stack || e);
        return message.reply("❌ Deu erro ao desfazer. Veja os logs do Render.");
      } finally {
        dbClient.release();
      }

      const afterUser = await rollToToday(message.guild.id, h.userId);

      await insertHistorico({
        guildId: message.guild.id,
        userId: h.userId,
        tipo: h.tipo,
        quantidade: 0,
        status: "DESFEITO",
        msgId: message.id,
        gerenteId: message.author.id,
        aplicado: 0,
        carry: 0,
        dia: todayKey(),
      });

      try {
        const refMsg = await message.channel.messages.fetch(refMsgId);
        if (refMsg?.author?.id === client.user.id) {
          await refMsg
            .edit({
              content:
                `🔁 **DESFEITO**\n` +
                `📦 ${h.tipo.toUpperCase()} • 0\n` +
                `➡️ Aplicado hoje: **0** | Extra (amanhã): **0**\n\n` +
                `👤 Usuário: <@${h.userId}>\n` +
                `🛡️ Desfeito por: <@${message.author.id}>`,
              components: [],
            })
            .catch(() => null);
        }
      } catch {}

      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle("🔁 Farme desfeito com sucesso")
        .setDescription(
          `👤 Usuário: <@${h.userId}>\n` +
          `🧾 Tipo: **${h.tipo.toUpperCase()}**\n` +
          `📦 Revertido para: **0**\n` +
          `🛡️ Desfeito por: <@${message.author.id}>\n` +
          `📅 Dia: **${dia}**`
        )
        .setTimestamp();

      const thumb = getThumb(client);
      if (thumb) embed.setThumbnail(thumb);

      await message.reply({ embeds: [embed] });

      const logChannel = getLogChannel(message.guild);
      if (logChannel) {
        const bP = Number(beforeUser?.papelHoje || 0), bPC = Number(beforeUser?.papelCarry || 0);
        const bS = Number(beforeUser?.sementesHoje || 0), bSC = Number(beforeUser?.sementesCarry || 0);
        const aP = Number(afterUser?.papelHoje || 0), aPC = Number(afterUser?.papelCarry || 0);
        const aS = Number(afterUser?.sementesHoje || 0), aSC = Number(afterUser?.sementesCarry || 0);

        const logEmbed = new EmbedBuilder()
          .setColor("#ff4d4d")
          .setTitle(`🚨 DESFEITO (00) — ${cfg.NAME}`)
          .setDescription(
            `👤 Usuário: <@${h.userId}>\n` +
            `🧾 Tipo: **${h.tipo.toUpperCase()}**\n\n` +
            `🔎 **ANTES**\n` +
            `📄 Papel: ${bP}/100 (extra: ${bPC})\n` +
            `🌱 Sementes: ${bS}/100 (extra: ${bSC})\n\n` +
            `✅ **DEPOIS**\n` +
            `📄 Papel: ${aP}/100 (extra: ${aPC})\n` +
            `🌱 Sementes: ${aS}/100 (extra: ${aSC})\n\n` +
            `🛡️ Por: <@${message.author.id}>`
          )
          .setTimestamp();

        if (thumb) logEmbed.setThumbnail(thumb);
        await logChannel.send({ embeds: [logEmbed] }).catch(() => null);
      }

      return;
    }

    // ==========================
    // 📊 !relatorio
    // ==========================
    if (lower === "!relatorio") {
      if (!isGerente && !is00) return message.reply("❌ Sem permissão.");

      const dia = todayKey();
      const resumo = await getResumoGeralDia(message.guild.id, dia);
      const tabela = await getTabelaResumoDia(message.guild.id, dia);

      const totalGeral = (resumo.papel_total || 0) + (resumo.sementes_total || 0);
      const top5 = tabela.slice(0, 5);

      const topTxt = top5.length
        ? top5.map((r, i) => `${i + 1}. <@${r.userId}> — ✅ Total: **${r.total}** (📄 ${r.papel} | 🌱 ${r.sementes})`).join("\n")
        : "Nenhum farme aprovado hoje.";

      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle(`📊 Relatório Geral — ${cfg.NAME}`)
        .setDescription(
          `📅 Dia: **${dia}**\n\n` +
            `👥 Membros com farme aprovado: **${resumo.total_membros}**\n` +
            `📄 Total Papel aprovado: **${resumo.papel_total}**\n` +
            `🌱 Total Sementes aprovadas: **${resumo.sementes_total}**\n` +
            `📦 Total Geral aprovado: **${totalGeral}**\n\n` +
            `🏆 **Top 5 do dia:**\n${topTxt}`
        )
        .setTimestamp();

      const thumb = getThumb(client);
      if (thumb) embed.setThumbnail(thumb);

      return message.reply({ embeds: [embed] });
    }

    // ==========================
    // 🚫 !naofarmou
    // ==========================
    if (lower.startsWith("!naofarmou")) {
      if (!isGerente && !is00) return message.reply("❌ Sem permissão.");

      const parts = content.split(/\s+/);
      const roleArg = parts.slice(1).join(" ").trim();
      const role = resolveRoleFromMessage(message, roleArg);

      await message.guild.members.fetch();

      const dia = todayKey();
      const aprovadosSet = await getAprovadosUserIdsDia(message.guild.id, dia);

      const allMembers = Array.from(message.guild.members.cache.values()).filter((m) => !m.user.bot);
      const scopedMembers = role ? allMembers.filter((m) => m.roles.cache.has(role.id)) : allMembers;

      const naoFarmaram = scopedMembers.filter((m) => !aprovadosSet.has(String(m.user.id)));

      const scopeTxt = role ? `Cargo: **${role.name}**` : "Cargo: **Todos**";
      const header =
        `📅 Dia: **${dia}**\n` +
        `🎯 ${scopeTxt}\n` +
        `👥 Total analisado: **${scopedMembers.length}**\n` +
        `🚫 Sem farme aprovado: **${naoFarmaram.length}**`;

      if (!naoFarmaram.length) {
        const embed = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle(`🚫 Não farmou (aprovado) — ${cfg.NAME}`)
          .setDescription(`${header}\n\n✅ Todo mundo desse filtro teve farme aprovado hoje.`)
          .setTimestamp();

        const thumb = getThumb(client);
        if (thumb) embed.setThumbnail(thumb);

        return message.reply({ embeds: [embed] });
      }

      const lines = naoFarmaram.map((m, i) => `${i + 1}. <@${m.user.id}>`).slice(0, 300);
      const chunks = chunkTextLines(lines, 3800);
      const thumb = getThumb(client);

      for (let idx = 0; idx < chunks.length; idx++) {
        const embed = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle(`🚫 Não farmou (aprovado) — ${cfg.NAME}`)
          .setDescription(`${header}\n\n${chunks[idx]}`)
          .setTimestamp();

        if (thumb) embed.setThumbnail(thumb);

        if (idx === 0) await message.reply({ embeds: [embed] });
        else await message.channel.send({ embeds: [embed] });
      }
      return;
    }

    // ==========================
    // 📊 !tabela
    // ==========================
    if (lower.startsWith("!tabela")) {
      const parts = content.split(/\s+/);
      const arg = (parts[1] || "hoje").toLowerCase();

      let dia = todayKey();
      let tituloDia = "Hoje";
      if (arg === "ontem") {
        dia = yesterdayKey();
        tituloDia = "Ontem";
      }

      const rows = await getTabelaResumoDia(message.guild.id, dia);

      if (!rows.length) {
        return message.reply(`📊 **Tabela (${tituloDia})**\nNenhum farme **aprovado** em **${dia}**.`);
      }

      const lines = rows.map(
        (r, i) => `${i + 1}. <@${r.userId}> — 📄 **${r.papel}** | 🌱 **${r.sementes}** | ✅ Total: **${r.total}**`
      );
      const thumb = getThumb(client);
      const chunks = chunkTextLines(lines, 3800);

      for (let idx = 0; idx < chunks.length; idx++) {
        const embed = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle(`📊 Tabela de Farmes Aprovados — ${cfg.NAME}`)
          .setDescription(`📅 Dia: **${dia}** (${tituloDia})\n\n${chunks[idx]}`)
          .setTimestamp();

        if (thumb) embed.setThumbnail(thumb);

        if (idx === 0) await message.reply({ embeds: [embed] });
        else await message.channel.send({ embeds: [embed] });
      }
      return;
    }

    // ==========================
    // !status
    // ==========================
    if (lower === "!status") {
      const u = await rollToToday(message.guild.id, message.author.id);
      if (!u) return message.reply("❌ Erro interno (usuário inválido).");

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

    // ==========================
    // !farme (somente canal ENVIO)
    // ==========================
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

    void isGerente;
    void is00;
  } catch (e) {
    console.error("Erro messageCreate:", e?.stack || e);
    try {
      await message.reply("❌ Erro interno.");
    } catch {}
  }
});

// ==========================
// 🔘 BUTTONS
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

    const parts = interaction.customId.split("_");
    const acao = parts[0];
    const userId = parts[1];
    const quantidade = parseInt(parts[2], 10);
    const tipo = parts[3];
    const msgId = interaction.message?.id;

    if (!msgId) return interaction.reply({ content: "⚠️ Não consegui pegar o ID da mensagem.", ephemeral: true });

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

    const logChannel = getLogChannel(interaction.guild);

    const sendLogEmbed = async (title, description) => {
      if (!logChannel) return;
      const embed = new EmbedBuilder().setColor("#2b2d31").setTitle(title).setDescription(description).setTimestamp();
      const thumb = getThumb(interaction);
      if (thumb) embed.setThumbnail(thumb);
      await logChannel.send({ embeds: [embed] }).catch(() => null);
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

      await interaction.message
        .edit({
          content:
            `✅ **Aprovado**\n` +
            `📦 ${tipo.toUpperCase()} • ${quantidade}\n` +
            `➡️ Aplicado hoje: **${result.aplicado}** | Extra (amanhã): **${result.carry}**\n\n` +
            `📄 Papel: **${u.papelHoje}/100** (extra: ${u.papelCarry})\n` +
            `🌱 Sementes: **${u.sementesHoje}/100** (extra: ${u.sementesCarry})\n\n` +
            `📝 (Para o 00) Reply aqui e use: **!editar 10** ou **!desfazer**`,
          components: [],
        })
        .catch(() => null);

      await sendLogEmbed(
        `✅ FARME APROVADO — ${cfg.NAME}`,
        `👤 Usuário: <@${userId}>\n🧾 Tipo: **${tipo.toUpperCase()}**\n📦 Quantidade: **${quantidade}**\n🛡️ Aprovado por: <@${interaction.user.id}>`
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

      await interaction.message
        .edit({
          content: `❌ **Negado**\n📦 ${tipo.toUpperCase()} • ${quantidade}`,
          components: [],
        })
        .catch(() => null);

      await sendLogEmbed(
        `❌ FARME NEGADO — ${cfg.NAME}`,
        `👤 Usuário: <@${userId}>\n🧾 Tipo: **${tipo.toUpperCase()}**\n📦 Quantidade: **${quantidade}**\n🛡️ Negado por: <@${interaction.user.id}>`
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