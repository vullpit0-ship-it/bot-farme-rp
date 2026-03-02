const {
  Client,
  GatewayIntentBits,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  Partials,
} = require("discord.js");

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

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Supabase normalmente precisa SSL em host externo
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id TEXT PRIMARY KEY,
      "ultimoDia" TEXT,
      "entregueHoje" INTEGER,
      divida INTEGER
    );

    CREATE TABLE IF NOT EXISTS historico (
      id BIGSERIAL PRIMARY KEY,
      "userId" TEXT,
      tipo TEXT,
      quantidade INTEGER,
      status TEXT,
      data TEXT,
      "msgId" TEXT,
      "gerenteId" TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_historico_msgId ON historico ("msgId");
  `);

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

client.once("ready", () => {
  console.log("Bot online como", client.user.tag);
});

// ==========================
// 🧠 HELPERS
// ==========================
function nowBR() {
  return new Date().toLocaleString("pt-BR");
}

function todayKey() {
  return new Date().toDateString();
}

function getLogChannel(guild) {
  return guild.channels.cache.get(LOG_CHANNEL_ID) || null;
}

function isEnvioChannel(message) {
  return (
    message.channel.id === ENVIO_FARME_CHANNEL_ID ||
    message.channel.parentId === ENVIO_FARME_CHANNEL_ID
  );
}

// ✅ garante usuário (sem quebrar com concorrência)
async function ensureUser(userId) {
  // tenta buscar
  let res = await pool.query(`SELECT * FROM usuarios WHERE id = $1`, [userId]);
  if (res.rows[0]) return res.rows[0];

  // tenta inserir (se outro processo inserir ao mesmo tempo, não quebra)
  const hoje = todayKey();
  await pool.query(
    `INSERT INTO usuarios (id, "ultimoDia", "entregueHoje", divida)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (id) DO NOTHING`,
    [userId, hoje, 0, 0]
  );

  // busca de novo e retorna
  res = await pool.query(`SELECT * FROM usuarios WHERE id = $1`, [userId]);
  return res.rows[0];
}

async function setEntregueHoje(userId, novoValor) {
  await pool.query(`UPDATE usuarios SET "entregueHoje" = $1 WHERE id = $2`, [
    novoValor,
    userId,
  ]);
}

async function insertHistorico({ userId, tipo, quantidade, status, msgId, gerenteId }) {
  await pool.query(
    `INSERT INTO historico ("userId", tipo, quantidade, status, data, "msgId", "gerenteId")
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [userId, tipo, quantidade, status, nowBR(), msgId, gerenteId]
  );
}

// evita clicar duas vezes no mesmo farme
async function alreadyProcessed(msgId) {
  const { rows } = await pool.query(
    `SELECT id FROM historico
     WHERE "msgId" = $1 AND (status = 'APROVADO' OR status = 'NEGADO')
     LIMIT 1`,
    [msgId]
  );
  return !!rows[0];
}

// ==========================
// 📩 MESSAGE
// ==========================
client.on("messageCreate", async (message) => {
  try {
    if (!message.guild || message.author.bot) return;

    const member = await message.guild.members.fetch(message.author.id);
    const isGerente = member.roles.cache.has(GERENTE_ROLE_ID);
    const is00 = member.roles.cache.has(ROLE_00_ID);

    // ======================
    // 🔥 FARME
    // ======================
    if (message.content.startsWith("!farme")) {
      if (!isEnvioChannel(message)) return;

      const args = message.content.split(" ");
      const tipo = args[1]?.toLowerCase();
      const quantidade = parseInt(args[2], 10);

      if (!["sementes", "papel"].includes(tipo)) {
        return message.reply("❌ Tipo inválido. Use `sementes` ou `papel`.");
      }

      if (isNaN(quantidade) || quantidade <= 0) {
        return message.reply("❌ Quantidade inválida. Ex: `!farme papel 10`");
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
        content: `📥 Farme enviado por ${message.author}\n📦 ${tipo.toUpperCase()} • ${quantidade}\n⏳ Aguardando gerente...`,
        components: [row],
      });
    }

    // ======================
    // ✏️ EDITAR (SO 00)
    // ======================
    if (message.content.startsWith("!editar")) {
      if (!is00) return message.reply("❌ Apenas cargo **00** pode usar.");

      const user = message.mentions.users.first();
      const valor = parseInt(message.content.split(" ")[2], 10);

      if (!user || isNaN(valor)) {
        return message.reply("Use: `!editar @usuario +50` ou `!editar @usuario -50`");
      }

      if (user.id === message.author.id) {
        return message.reply("❌ Você não pode editar o próprio farme.");
      }

      const u = await ensureUser(user.id);

      let novo = (u.entregueHoje || 0) + valor;
      if (novo < 0) novo = 0;

      await setEntregueHoje(user.id, novo);

      await insertHistorico({
        userId: user.id,
        tipo: "ajuste",
        quantidade: valor,
        status: "AJUSTE",
        msgId: "manual",
        gerenteId: message.author.id,
      });

      return message.reply(`✅ Atualizado: <@${user.id}> agora está com **${novo}/100**`);
    }

    // (opcional) se quiser, você pode bloquear comandos para gerente etc. aqui
    void isGerente; // evita warning caso não use
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
      return interaction.followUp({
        content: "⚠️ Esse farme já foi processado.",
        ephemeral: true,
      });
    }

    const u = await ensureUser(userId);

    if (acao === "aprovar") {
      const novo = (u.entregueHoje || 0) + quantidade;

      await setEntregueHoje(userId, novo);

      await insertHistorico({
        userId,
        tipo,
        quantidade,
        status: "APROVADO",
        msgId,
        gerenteId: interaction.user.id,
      });

      await interaction.message.edit({
        content: `✅ Farme aprovado! (novo: ${novo}/100)`,
        components: [],
      });

      if (logChannel) {
        logChannel
          .send(
            `✅ APROVADO: <@${userId}> +${quantidade} (${tipo}) | Novo: ${novo}/100 | Por: <@${interaction.user.id}>`
          )
          .catch(() => null);
      }
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
      });

      await interaction.message.edit({
        content: "❌ Farme negado!",
        components: [],
      });

      if (logChannel) {
        logChannel
          .send(
            `❌ NEGADO: <@${userId}> ${quantidade} (${tipo}) | Por: <@${interaction.user.id}>`
          )
          .catch(() => null);
      }
      return;
    }

    // se vier algo inesperado:
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

    if (!process.env.DISCORD_TOKEN) {
      console.error("Faltando DISCORD_TOKEN nas env vars (Render).");
      process.exit(1);
    }

    await client.login(process.env.DISCORD_TOKEN);
  } catch (e) {
    console.error("Falha ao iniciar:", e);
    process.exit(1);
  }
})();