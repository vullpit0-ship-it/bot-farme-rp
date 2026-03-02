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

    CREATE INDEX IF NOT EXISTS idx_historico_msgId ON historico ("msgId");
    CREATE INDEX IF NOT EXISTS idx_historico_dia ON historico (dia);
  `);

  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "papelHoje" INTEGER;`);
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "sementesHoje" INTEGER;`);
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "papelCarry" INTEGER;`);
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "sementesCarry" INTEGER;`);
  await pool.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS "ultimoDia" TEXT;`);

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

// ✅ discord.js v15: evento correto
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

async function rollToToday(userId) {
  const hoje = todayKey();
  const u = await ensureUser(userId);

  if (u.ultimoDia === hoje) return u;

  const papelCarry = Number(u.papelCarry || 0);
  const sementesCarry = Number(u.sementesCarry || 0);

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

async function insertHistorico({
  userId,
  tipo,
  quantidade,
  status,
  msgId,
  gerenteId,
  aplicado,
  carry,
  dia,
}) {
  await pool.query(
    `INSERT INTO historico ("userId", tipo, quantidade, status, data, dia, "msgId", "gerenteId", aplicado, carry)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [
      userId,
      tipo,
      quantidade,
      status,
      nowBR(),
      dia || todayKey(),
      msgId,
      gerenteId,
      aplicado ?? null,
      carry ?? null,
    ]
  );
}

async function alreadyProcessed(msgId) {
  const { rows } = await pool.query(
    `SELECT id FROM historico WHERE "msgId" = $1 AND (status='APROVADO' OR status='NEGADO') LIMIT 1`,
    [msgId]
  );
  return !!rows[0];
}

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

    await pool.query(
      `UPDATE usuarios SET "sementesHoje"=$1, "sementesCarry"=$2 WHERE id=$3`,
      [sementesHoje, sementesCarry, userId]
    );
  } else {
    throw new Error("tipo inválido");
  }

  const u1 = await pool.query(`SELECT * FROM usuarios WHERE id = $1`, [userId]);
  return { user: u1.rows[0], aplicado, carry };
}

// ==========================
// 📩 MESSAGE
// ==========================
client.on("messageCreate", async (message) => {
  try {
    if (!message.guild || message.author.bot) return;

    // ✅ LOG IMPORTANTE (pra confirmar que chega no bot)
    console.log("[MSG]", message.channel.id, message.author.tag, JSON.stringify(message.content));

    const member = await message.guild.members.fetch(message.author.id);
    const isGerente = member.roles.cache.has(GERENTE_ROLE_ID);
    const is00 = member.roles.cache.has(ROLE_00_ID);

    const content = (message.content || "").trim();
    const lower = content.toLowerCase();

    // ======================
    // 🎛 PAINEL (robusto)
    // ======================
    if (lower === "!painel") {
      const u = await rollToToday(message.author.id);

      const txt =
        `👤 ${message.author}\n\n` +
        `📄 **Papel:** ${u.papelHoje}/100 (carry: ${u.papelCarry})\n` +
        `🌱 **Sementes:** ${u.sementesHoje}/100 (carry: ${u.sementesCarry})\n\n` +
        `🕒 Dia: **${u.ultimoDia}**\n` +
        `✅ Use \`!farme papel 10\` ou \`!farme sementes 10\` no canal de envio.`;

      try {
        const embed = new EmbedBuilder()
          .setColor("#2b2d31")
          .setTitle("📌 Painel do Farme")
          .setDescription(txt)
          .setFooter({
            text: is00 ? "Você é 00 (tem !editar)" : isGerente ? "Você é Gerente" : "Membro",
          })
          .setTimestamp();

        return message.reply({ embeds: [embed] });
      } catch (e) {
        console.error("Falha ao enviar embed do painel:", e);
        return message.reply({ content: `📌 **Painel do Farme**\n\n${txt}` });
      }
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
        let novo = Math.max(0, Number(u.papelHoje || 0) + valor);
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
        let novo = Math.max(0, Number(u.sementesHoje || 0) + valor);
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
          `➡️ Aplicado hoje: **${result.aplicado}** | Carry (amanhã): **${result.carry}**\n\n` +
          `📄 Papel: **${u.papelHoje}/100** (carry: ${u.papelCarry})\n` +
          `🌱 Sementes: **${u.sementesHoje}/100** (carry: ${u.sementesCarry})`,
        components: [],
      });

      logChannel
        ?.send(
          `✅ APROVADO: <@${userId}> +${quantidade} (${tipo}) | aplicado ${result.aplicado} / carry ${result.carry} | Por: <@${interaction.user.id}>`
        )
        .catch(() => null);

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

      logChannel
        ?.send(`❌ NEGADO: <@${userId}> ${quantidade} (${tipo}) | Por: <@${interaction.user.id}>`)
        .catch(() => null);

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
// build-force 2026-03-02T03:19:02.4759051-03:00

