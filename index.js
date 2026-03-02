const {
  Client,
  GatewayIntentBits,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  EmbedBuilder,
  Partials,
} = require("discord.js");

console.log("🔥 NOVA BUILD CARREGADA 🔥", new Date().toISOString());

const express = require("express");
const { Pool } = require("pg");

// ==========================
// ✅ IDS FIXOS
// ==========================
const GERENTE_ROLE_ID = "1477779548484538539";
const ROLE_00_ID = "1477850489189044365";

const LOG_CHANNEL_ID = "1477800551340310651";
const ENVIO_FARME_CHANNEL_ID = "1477777883714818098";

// ✅ Opcional: logo custom pro log (Render -> Environment -> LOGO_URL)
const LOGO_URL = process.env.LOGO_URL || null;

// ==========================
// 🌐 WEB
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
function tomorrowKey() {
  const d = new Date();
  d.setDate(d.getDate() + 1);
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
// 🧱 INIT DB (COM MIGRAÇÃO)
// ==========================
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id TEXT PRIMARY KEY,
      "ultimoDia" TEXT,
      "papelHoje" INTEGER,
      "sementesHoje" INTEGER,
      "papelCarry" INTEGER,
      "sementesCarry" INTEGER
    );

    CREATE TABLE IF NOT EXISTS historico (
      id BIGSERIAL PRIMARY KEY,
      "userId" TEXT,
      tipo TEXT,
      quantidade INTEGER,
      status TEXT,
      data TEXT,
      dia TEXT,
      "msgId" TEXT,
      "gerenteId" TEXT,
      aplicado INTEGER,
      carry INTEGER
    );
  `);

  // usuarios
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "papelHoje" INTEGER;`);
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "sementesHoje" INTEGER;`);
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "papelCarry" INTEGER;`);
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "sementesCarry" INTEGER;`);
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "ultimoDia" TEXT;`);

  // historico
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS "userId" TEXT;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS tipo TEXT;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS quantidade INTEGER;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS status TEXT;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS data TEXT;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS dia TEXT;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS "msgId" TEXT;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS "gerenteId" TEXT;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS aplicado INTEGER;`);
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS carry INTEGER;`);

  // ranking semanal
  await pool.query(`ALTER TABLE historico ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();`);
  await pool.query(`UPDATE historico SET created_at = NOW() WHERE created_at IS NULL;`);

  // defaults
  await pool.query(
    `
    UPDATE usuarios
    SET
      "papelHoje" = COALESCE("papelHoje", 0),
      "sementesHoje" = COALESCE("sementesHoje", 0),
      "papelCarry" = COALESCE("papelCarry", 0),
      "sementesCarry" = COALESCE("sementesCarry", 0),
      "ultimoDia" = COALESCE("ultimoDia", $1)
  `,
    [todayKey()]
  );

  // índices
  try {
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_msgId ON historico ("msgId");`);
  } catch (e) {
    console.error("Aviso: falha ao criar idx_historico_msgId:", e?.message || e);
  }
  try {
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_dia ON historico ("dia");`);
  } catch (e) {
    console.error("Aviso: falha ao criar idx_historico_dia:", e?.message || e);
  }
  try {
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_historico_created_at ON historico (created_at);`);
  } catch (e) {
    console.error("Aviso: falha ao criar idx_historico_created_at:", e?.message || e);
  }

  console.log("DB OK (PostgreSQL / Supabase)");
}

// ==========================
// 🤖 CLIENT
// ==========================
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Channel],
});

client.once("clientReady", () => {
  console.log("Bot online como", client.user?.tag);
});

// ==========================
// 🧠 HELPERS
// ==========================
function getLogChannel(guild) {
  return guild.channels.cache.get(LOG_CHANNEL_ID) || null;
}

function isEnvioChannel(message) {
  return (
    message.channel.id === ENVIO_FARME_CHANNEL_ID ||
    message.channel.parentId === ENVIO_FARME_CHANNEL_ID
  );
}

async function ensureUser(userId) {
  let res = await pool.query(`SELECT * FROM usuarios WHERE id = $1`, [userId]);
  if (res.rows[0]) return res.rows[0];

  const hoje = todayKey();
  await pool.query(
    `INSERT INTO usuarios (id, "ultimoDia", "papelHoje", "sementesHoje", "papelCarry", "sementesCarry")
     VALUES ($1, $2, 0, 0, 0, 0)
     ON CONFLICT (id) DO NOTHING`,
    [userId, hoje]
  );

  res = await pool.query(`SELECT * FROM usuarios WHERE id = $1`, [userId]);
  return res.rows[0];
}

// quando vira o dia: começa o dia com o carry (excesso de ontem)
async function rollToToday(userId) {
  const hoje = todayKey();
  const u = await ensureUser(userId);

  if (u.ultimoDia === hoje) return u;

  const papelCarry = Number(u.papelCarry || 0);
  const sementesCarry = Number(u.sementesCarry || 0);

  // entra no novo dia já contando extra (até 100), resto continua em extra
  const papelHoje = Math.min(100, papelCarry);
  const sementesHoje = Math.min(100, sementesCarry);

  const novoPapelCarry = Math.max(0, papelCarry - 100);
  const novoSementesCarry = Math.max(0, sementesCarry - 100);

  await pool.query(
    `UPDATE usuarios
     SET "ultimoDia" = $1,
         "papelHoje" = $2,
         "sementesHoje" = $3,
         "papelCarry" = $4,
         "sementesCarry" = $5
     WHERE id = $6`,
    [hoje, papelHoje, sementesHoje, novoPapelCarry, novoSementesCarry, userId]
  );

  const res = await pool.query(`SELECT * FROM usuarios WHERE id = $1`, [userId]);
  return res.rows[0];
}

async function insertHistorico({ userId, tipo, quantidade, status, msgId, gerenteId, aplicado, carry, dia }) {
  await pool.query(
    `INSERT INTO historico ("userId", tipo, quantidade, status, data, dia, "msgId", "gerenteId", aplicado, carry)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [userId, tipo, quantidade, status, nowBR(), dia || todayKey(), msgId, gerenteId, aplicado ?? null, carry ?? null]
  );
}

async function alreadyProcessed(msgId) {
  const { rows } = await pool.query(
    `SELECT id FROM historico WHERE "msgId" = $1 AND (status='APROVADO' OR status='NEGADO') LIMIT 1`,
    [msgId]
  );
  return !!rows[0];
}

// aplica aprovação respeitando limite 100/dia por tipo e joga excesso para extra
async function applyFarm(userId, tipo, quantidade) {
  const u0 = await rollToToday(userId);

  let papelHoje = Number(u0.papelHoje || 0);
  let sementesHoje = Number(u0.sementesHoje || 0);
  let papelCarry = Number(u0.papelCarry || 0);
  let sementesCarry = Number(u0.sementesCarry || 0);

  let aplicado = 0;
  let carry = 0;

  if (tipo === "papel") {
    const restante = Math.max(0, 100 - papelHoje);
    aplicado = Math.min(quantidade, restante);
    carry = Math.max(0, quantidade - aplicado);

    papelHoje += aplicado;
    papelCarry += carry;

    await pool.query(`UPDATE usuarios SET "papelHoje"=$1, "papelCarry"=$2 WHERE id=$3`, [
      papelHoje,
      papelCarry,
      userId,
    ]);
  } else if (tipo === "sementes") {
    const restante = Math.max(0, 100 - sementesHoje);
    aplicado = Math.min(quantidade, restante);
    carry = Math.max(0, quantidade - aplicado);

    sementesHoje += aplicado;
    sementesCarry += carry;

    await pool.query(`UPDATE usuarios SET "sementesHoje"=$1, "sementesCarry"=$2 WHERE id=$3`, [
      sementesHoje,
      sementesCarry,
      userId,
    ]);
  } else {
    throw new Error("tipo inválido");
  }

  const u1 = await pool.query(`SELECT * FROM usuarios WHERE id = $1`, [userId]);
  return { user: u1.rows[0], aplicado, carry };
}

// ==========================
// 📊 RANKING APROVAÇÕES (Gerente/00)
// ==========================
async function getRankingAprovacoesDia(diaStr) {
  const { rows } = await pool.query(
    `
    SELECT "gerenteId",
           COUNT(*)::int AS aprovacoes,
           COALESCE(SUM(COALESCE(aplicado,0)),0)::int AS aplicado_total,
           COALESCE(SUM(COALESCE(carry,0)),0)::int AS extra_total
    FROM historico
    WHERE status = 'APROVADO'
      AND "gerenteId" IS NOT NULL
      AND dia = $1
    GROUP BY "gerenteId"
    ORDER BY aprovacoes DESC, aplicado_total DESC
    LIMIT 10
  `,
    [diaStr]
  );
  return rows;
}

async function getRankingAprovacoesSemana() {
  const { rows } = await pool.query(
    `
    SELECT "gerenteId",
           COUNT(*)::int AS aprovacoes,
           COALESCE(SUM(COALESCE(aplicado,0)),0)::int AS aplicado_total,
           COALESCE(SUM(COALESCE(carry,0)),0)::int AS extra_total
    FROM historico
    WHERE status = 'APROVADO'
      AND "gerenteId" IS NOT NULL
      AND created_at >= NOW() - INTERVAL '7 days'
    GROUP BY "gerenteId"
    ORDER BY aprovacoes DESC, aplicado_total DESC
    LIMIT 10
  `
  );
  return rows;
}

function formatRankingAprov(rows) {
  if (!rows || rows.length === 0) return "Nenhuma aprovação encontrada.";
  return rows
    .map((r, i) => {
      const user = `<@${r.gerenteId}>`;
      return `**${i + 1}.** ${user} — ✅ ${r.aprovacoes} | aplicado **${r.aplicado_total}** | extra **${r.extra_total}**`;
    })
    .join("\n");
}

// ==========================
// 🏆 RANKING GERAL (Papel/Sementes)
// ==========================
// Top 10 por tipo, por DIA
async function getRankingTipoDia(diaStr, tipo) {
  const { rows } = await pool.query(
    `
    SELECT "userId",
           COALESCE(SUM(COALESCE(aplicado,0)),0)::int AS aplicado_total,
           COALESCE(SUM(COALESCE(carry,0)),0)::int AS extra_total
    FROM historico
    WHERE status='APROVADO'
      AND tipo=$2
      AND dia=$1
    GROUP BY "userId"
    ORDER BY aplicado_total DESC, extra_total DESC
    LIMIT 10
  `,
    [diaStr, tipo]
  );
  return rows;
}

// Top 10 por tipo, por SEMANA (7 dias)
async function getRankingTipoSemana(tipo) {
  const { rows } = await pool.query(
    `
    SELECT "userId",
           COALESCE(SUM(COALESCE(aplicado,0)),0)::int AS aplicado_total,
           COALESCE(SUM(COALESCE(carry,0)),0)::int AS extra_total
    FROM historico
    WHERE status='APROVADO'
      AND tipo=$1
      AND created_at >= NOW() - INTERVAL '7 days'
    GROUP BY "userId"
    ORDER BY aplicado_total DESC, extra_total DESC
    LIMIT 10
  `,
    [tipo]
  );
  return rows;
}

function formatRankingUsers(rows) {
  if (!rows || rows.length === 0) return "Sem dados ainda.";
  return rows
    .map((r, i) => `**${i + 1}.** <@${r.userId}> — aplicado **${r.aplicado_total}** | extra **${r.extra_total}**`)
    .join("\n");
}

// ==========================
// 📩 MESSAGE
// ==========================
client.on("messageCreate", async (message) => {
  try {
    if (!message.guild || message.author.bot) return;

    console.log("[MSG]", message.channel.id, message.author.tag, JSON.stringify(message.content));

    const member = await message.guild.members.fetch(message.author.id);
    const isGerente = member.roles.cache.has(GERENTE_ROLE_ID);
    const is00 = member.roles.cache.has(ROLE_00_ID);

    const content = (message.content || "").trim();
    const lower = content.toLowerCase();

    // ======================
    // 🎛 PAINEL
    // ======================
    if (lower === "!painel") {
      const u = await rollToToday(message.author.id);

      const txt =
        `👤 ${message.author}\n\n` +
        `📄 **Papel:** ${u.papelHoje}/100 (extra: ${u.papelCarry})\n` +
        `🌱 **Sementes:** ${u.sementesHoje}/100 (extra: ${u.sementesCarry})\n\n` +
        `🕒 Dia: **${u.ultimoDia}**\n` +
        `✅ Use \`!farme papel 10\` ou \`!farme sementes 10\` no canal de envio.`;

      try {
        const embed = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle("📌 Painel do Farme")
          .setDescription(txt)
          .setFooter({ text: is00 ? "Você é 00 (tem !editar)" : isGerente ? "Você é Gerente" : "Membro" })
          .setTimestamp();

        const thumb = getThumb(client);
        if (thumb) embed.setThumbnail(thumb);

        return message.reply({ embeds: [embed] });
      } catch (e) {
        console.error("Falha ao enviar embed do painel:", e);
        return message.reply({ content: `📌 **Painel do Farme**\n\n${txt}` });
      }
    }

    // ======================
    // 🏆 RANKING GERAL (QUALQUER UM)
    // ======================
    if (lower === "!ranking") {
      const hoje = todayKey();

      const [papelDia, sementesDia, papelSemana, sementesSemana] = await Promise.all([
        getRankingTipoDia(hoje, "papel"),
        getRankingTipoDia(hoje, "sementes"),
        getRankingTipoSemana("papel"),
        getRankingTipoSemana("sementes"),
      ]);

      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle("🏆 Ranking de Farme (Top 10)")
        .addFields(
          { name: `📅 PAPEL — Hoje (${hoje})`, value: formatRankingUsers(papelDia) },
          { name: `📅 SEMENTES — Hoje (${hoje})`, value: formatRankingUsers(sementesDia) },
          { name: "🗓️ PAPEL — Últimos 7 dias", value: formatRankingUsers(papelSemana) },
          { name: "🗓️ SEMENTES — Últimos 7 dias", value: formatRankingUsers(sementesSemana) },
        )
        .setFooter({ text: "Comando: !ranking" })
        .setTimestamp();

      const thumb = getThumb(client);
      if (thumb) embed.setThumbnail(thumb);

      return message.reply({ embeds: [embed] });
    }

    // ======================
    // 📊 RANKING APROVAÇÕES (Gerente/00)
    // ======================
    if (lower === "!rankaprov" || lower === "!rankingaprov" || lower === "!rankingaprovacoes") {
      if (!isGerente && !is00) return message.reply("❌ Apenas **Gerente/00** pode usar.");

      const hoje = todayKey();
      const [rDia, rSemana] = await Promise.all([getRankingAprovacoesDia(hoje), getRankingAprovacoesSemana()]);

      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle("🏆 Ranking de Aprovações (Gerentes/00)")
        .addFields(
          { name: `📅 Hoje (${hoje})`, value: formatRankingAprov(rDia) },
          { name: "🗓️ Últimos 7 dias", value: formatRankingAprov(rSemana) }
        )
        .setFooter({ text: "Comando: !rankaprov" })
        .setTimestamp();

      const thumb = getThumb(client);
      if (thumb) embed.setThumbnail(thumb);

      return message.reply({ embeds: [embed] });
    }

    // ======================
    // 🔥 FARME
    // ======================
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

    // ======================
    // ✏️ EDITAR (SÓ 00)
    // ======================
    if (lower.startsWith("!editar")) {
      if (!is00) return message.reply("❌ Apenas cargo **00** pode usar.");

      const user = message.mentions.users.first();
      const parts = content.split(/\s+/);
      const tipo = (parts[2] || "").toLowerCase();
      const valor = parseInt(parts[3], 10);

      if (!user || !["papel", "sementes"].includes(tipo) || isNaN(valor)) {
        return message.reply("Use: `!editar @usuario papel +50` ou `!editar @usuario sementes -10`");
      }
      if (user.id === message.author.id) {
        return message.reply("❌ Você não pode editar o próprio farme.");
      }

      const u = await rollToToday(user.id);

      if (tipo === "papel") {
        const novo = Math.max(0, Number(u.papelHoje || 0) + valor);
        await pool.query(`UPDATE usuarios SET "papelHoje"=$1 WHERE id=$2`, [novo, user.id]);

        await insertHistorico({
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

        return message.reply(`✅ Papel atualizado: <@${user.id}> **${novo}/100**`);
      }

      if (tipo === "sementes") {
        const novo = Math.max(0, Number(u.sementesHoje || 0) + valor);
        await pool.query(`UPDATE usuarios SET "sementesHoje"=$1 WHERE id=$2`, [novo, user.id]);

        await insertHistorico({
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

        return message.reply(`✅ Sementes atualizado: <@${user.id}> **${novo}/100**`);
      }
    }

    void isGerente;
  } catch (e) {
    console.error("Erro messageCreate:", e);
    try { await message.reply("❌ Erro interno."); } catch {}
  }
});

// ==========================
// 🔘 BUTTONS
// ==========================
client.on("interactionCreate", async (interaction) => {
  try {
    if (!interaction.isButton() || !interaction.guild) return;

    const member = await interaction.guild.members.fetch(interaction.user.id);
    const isGerente = member.roles.cache.has(GERENTE_ROLE_ID);
    const is00 = member.roles.cache.has(ROLE_00_ID);

    if (!isGerente && !is00) {
      return interaction.reply({ content: "❌ Sem permissão.", ephemeral: true });
    }

    const logChannel = getLogChannel(interaction.guild);

    const [acao, userId, quantidadeStr, tipo] = interaction.customId.split("_");
    const quantidade = parseInt(quantidadeStr, 10);
    const msgId = interaction.message.id;

    await interaction.deferUpdate();

    if (await alreadyProcessed(msgId)) {
      return interaction.followUp({ content: "⚠️ Esse farme já foi processado.", ephemeral: true });
    }

    if (!["papel", "sementes"].includes(tipo) || isNaN(quantidade) || quantidade <= 0) {
      return interaction.followUp({ content: "⚠️ Dados inválidos nesse botão.", ephemeral: true });
    }

    const sendLogEmbed = async (title, description) => {
      if (!logChannel) return;
      const embed = new EmbedBuilder()
        .setColor("#2b2d31")
        .setTitle(title)
        .setDescription(description)
        .setTimestamp();

      const thumb = getThumb(interaction);
      if (thumb) embed.setThumbnail(thumb);

      try {
        await logChannel.send({ embeds: [embed] });
      } catch {
        await logChannel.send(description).catch(() => null);
      }
    };

    if (acao === "aprovar") {
      const result = await applyFarm(userId, tipo, quantidade);
      const u = result.user;

      await insertHistorico({
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

      await interaction.message.edit({
        content:
          `✅ **Aprovado**\n` +
          `📦 ${tipo.toUpperCase()} • ${quantidade}\n` +
          `➡️ Aplicado hoje: **${result.aplicado}** | Extra (amanhã): **${result.carry}**\n\n` +
          `📄 Papel: **${u.papelHoje}/100** (extra: ${u.papelCarry})\n` +
          `🌱 Sementes: **${u.sementesHoje}/100** (extra: ${u.sementesCarry})`,
        components: [],
      });

      await sendLogEmbed(
        "✅ FARME APROVADO",
        `👤 Usuário: <@${userId}>\n` +
          `🧾 Tipo: **${tipo.toUpperCase()}**\n` +
          `📦 Quantidade: **${quantidade}**\n` +
          `➡️ Aplicado: **${result.aplicado}** | Extra: **${result.carry}**\n` +
          `🛡️ Aprovado por: <@${interaction.user.id}>`
      );

      return;
    }

    if (acao === "negar") {
      await insertHistorico({
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

      await interaction.message.edit({
        content: `❌ **Negado**\n📦 ${tipo.toUpperCase()} • ${quantidade}`,
        components: [],
      });

      await sendLogEmbed(
        "❌ FARME NEGADO",
        `👤 Usuário: <@${userId}>\n` +
          `🧾 Tipo: **${tipo.toUpperCase()}**\n` +
          `📦 Quantidade: **${quantidade}**\n` +
          `🛡️ Negado por: <@${interaction.user.id}>`
      );

      return;
    }

    return interaction.followUp({ content: "⚠️ Ação inválida.", ephemeral: true });
  } catch (e) {
    console.error("Erro interactionCreate:", e);
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
    console.error("Falha ao iniciar:", e);
    process.exit(1);
  }
})();