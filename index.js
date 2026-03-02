const {
  Client,
  GatewayIntentBits,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  EmbedBuilder,
  StringSelectMenuBuilder,
  Partials,
} = require("discord.js");

const express = require("express");
const sqlite3 = require("sqlite3").verbose();

// ==========================
// 🌐 MINI WEB (Render Web Service precisa PORT)
// ==========================
const app = express();
app.get("/", (req, res) => res.send("Bot online ✅"));
app.listen(process.env.PORT || 3000, () => console.log("Web OK"));

// ==========================
// 🗄️ BANCO (SQLite) - INIT GARANTIDO
// ==========================
const db = new sqlite3.Database("./farmes.db", (err) => {
  if (err) console.error("Erro ao abrir DB:", err);
});

db.on("error", (err) => console.error("DB error:", err));

function initDB() {
  return new Promise((resolve, reject) => {
    db.exec(
      `
      PRAGMA journal_mode = WAL;

      CREATE TABLE IF NOT EXISTS usuarios (
        id TEXT PRIMARY KEY,
        ultimoDia TEXT,
        entregueHoje INTEGER,
        divida INTEGER
      );

      CREATE TABLE IF NOT EXISTS historico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        userId TEXT,
        tipo TEXT,
        quantidade INTEGER,
        status TEXT,
        data TEXT,
        msgId TEXT,
        gerenteId TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_historico_msgId ON historico(msgId);
      `,
      (err) => {
        if (err) {
          console.error("Erro criando tabelas:", err);
          return reject(err);
        }
        console.log("DB OK (tabelas prontas)");
        resolve();
      }
    );
  });
}

// ==========================
// 🤖 DISCORD CLIENT
// ==========================
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.DirectMessages,
  ],
  partials: [Partials.Channel],
});

client.once("ready", () => {
  console.log(`Bot online como ${client.user.tag}`);
});

// ==========================
// 🧠 Helpers
// ==========================
function getRoles(guild, member) {
  const gerenteRole = guild.roles.cache.find((r) => r.name === "Gerente");
  const role00 = guild.roles.cache.find((r) => r.name === "00");

  const isGerente = gerenteRole ? member.roles.cache.has(gerenteRole.id) : false;
  const is00 = role00 ? member.roles.cache.has(role00.id) : false;

  return { isGerente, is00 };
}

async function safeDM(user, content) {
  try {
    await user.send(content);
  } catch {
    // DM fechada
  }
}

function nowBR() {
  return new Date().toLocaleString("pt-BR");
}

function todayKey() {
  return new Date().toDateString();
}

// (Opcional) Defina um ID do canal de log no Render:
// LOG_CHANNEL_ID=1234567890
function getLogChannel(guild) {
  const byId = process.env.LOG_CHANNEL_ID
    ? guild.channels.cache.get(process.env.LOG_CHANNEL_ID)
    : null;

  if (byId) return byId;

  return guild.channels.cache.find((c) => c.name === "logs-farme") || null;
}

// =================================================
// 📩 MESSAGE
// =================================================
client.on("messageCreate", async (message) => {
  if (message.author.bot) return;
  if (!message.guild) return;

  const { isGerente, is00 } = getRoles(message.guild, message.member);

  // ==========================
  // 🎛 PAINEL
  // ==========================
  if (message.content === "!painel") {
    const embed = new EmbedBuilder()
      .setColor("#2b2d31")
      .setAuthor({
        name: "Central de Controle",
        iconURL: message.guild.iconURL(),
      })
      .setDescription("Selecione uma opção no menu abaixo.")
      .setFooter({ text: `Solicitado por ${message.author.username}` })
      .setTimestamp();

    const menu = new StringSelectMenuBuilder()
      .setCustomId("painel_menu")
      .setPlaceholder("Abrir menu...");

    const options = [
      { label: "Meu Saldo", value: "meu_saldo", emoji: "👤" },
      { label: "Meu Histórico", value: "meu_historico", emoji: "🧾" },
    ];

    if (isGerente || is00) {
      options.push({ label: "Devedores", value: "ver_devedores", emoji: "📋" });
    }
    if (is00) {
      options.push({ label: "Alterar Saldo", value: "alterar_saldo", emoji: "⚙️" });
    }

    menu.addOptions(options);

    const row = new ActionRowBuilder().addComponents(menu);
    return message.reply({ embeds: [embed], components: [row] });
  }

  // ==========================
  // ✏️ EDITAR (só 00)
  // ==========================
  if (message.content.startsWith("!editar")) {
    const role00 = message.guild.roles.cache.find((r) => r.name === "00");
    const is00local = role00 ? message.member.roles.cache.has(role00.id) : false;
    if (!is00local) return message.reply("❌ Apenas cargo **00** pode usar.");

    const user = message.mentions.users.first();
    const valor = parseInt(message.content.split(" ")[2], 10);

    if (!user || isNaN(valor)) {
      return message.reply("Use: `!editar @usuario +50` ou `!editar @usuario -50`");
    }

    const hojeDia = todayKey();

    db.get(`SELECT * FROM usuarios WHERE id = ?`, [user.id], (err, row) => {
      if (err) return message.reply("Erro no banco.");
      if (!row) return message.reply("Usuário sem registro ainda.");

      let entregueHoje = row.entregueHoje;
      let divida = row.divida;
      let ultimoDia = row.ultimoDia;

      if (ultimoDia !== hojeDia) {
        const falta = 100 - entregueHoje;
        if (falta > 0) divida += falta;
        entregueHoje = 0;
        ultimoDia = hojeDia;
      }

      const novoSaldo = Math.max(0, entregueHoje + valor);

      db.run(
        `UPDATE usuarios SET ultimoDia = ?, entregueHoje = ?, divida = ? WHERE id = ?`,
        [ultimoDia, novoSaldo, divida, user.id]
      );

      db.run(
        `INSERT INTO historico (userId, tipo, quantidade, status, data, msgId, gerenteId)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [user.id, "ajuste", valor, "AJUSTE", nowBR(), "manual", message.author.id]
      );

      return message.reply(
        `✅ Ajuste feito em <@${user.id}>.\n📊 Entregue hoje: **${novoSaldo}/100** | 💰 Dívida: **${divida}**`
      );
    });

    return;
  }

  // ==========================
  // 🔥 FARME
  // ==========================
  if (message.channel.name !== "envio-farme") return;
  if (!message.content.startsWith("!farme")) return;

  const args = message.content.split(" ");
  if (args.length < 3) return message.reply("Use: `!farme [sementes/papel] [quantidade]`");

  const tipo = (args[1] || "").toLowerCase();
  const quantidade = parseInt(args[2], 10);

  if (!["sementes", "papel"].includes(tipo)) return message.reply("❌ Tipo inválido.");
  if (isNaN(quantidade) || quantidade <= 0) return message.reply("❌ Quantidade inválida.");
  if (message.attachments.size === 0) return message.reply("❌ Envie o print junto com o comando.");

  const approveButton = new ButtonBuilder()
    .setCustomId(`aprovar_${message.author.id}_${quantidade}_${tipo}`)
    .setLabel("Aprovar")
    .setStyle(ButtonStyle.Success);

  const denyButton = new ButtonBuilder()
    .setCustomId(`negar_${message.author.id}_${quantidade}_${tipo}`)
    .setLabel("Negar")
    .setStyle(ButtonStyle.Danger);

  const row = new ActionRowBuilder().addComponents(approveButton, denyButton);

  return message.reply({
    content: `📥 Farme enviado por ${message.author}\n📦 ${tipo.toUpperCase()} • ${quantidade}\n⏳ Aguardando gerente...`,
    components: [row],
  });
});

// =================================================
// 🔘 INTERAÇÕES
// =================================================
client.on("interactionCreate", async (interaction) => {
  if (!interaction.guild) return;

  const { isGerente, is00 } = getRoles(interaction.guild, interaction.member);
  const logChannel = getLogChannel(interaction.guild);

  // ==========================
  // 🎛 MENU PAINEL
  // ==========================
  if (interaction.isStringSelectMenu() && interaction.customId === "painel_menu") {
    const escolha = interaction.values[0];

    if (escolha === "meu_saldo") {
      const hoje = todayKey();

      db.get(`SELECT * FROM usuarios WHERE id = ?`, [interaction.user.id], (err, row) => {
        if (!row) return interaction.reply({ content: "📊 Você ainda não tem registro.", ephemeral: true });

        let entregueHoje = row.entregueHoje;
        const divida = row.divida;

        if (row.ultimoDia !== hoje) entregueHoje = 0;

        const txt =
          `👤 **Seu status**\n\n` +
          `📦 Entregue hoje: **${entregueHoje}/100**\n` +
          `💰 Dívida: **${divida}**`;

        return interaction.reply({ content: txt, ephemeral: true });
      });

      return;
    }

    if (escolha === "meu_historico") {
      db.all(
        `SELECT * FROM historico WHERE userId = ? ORDER BY id DESC LIMIT 8`,
        [interaction.user.id],
        (err, rows) => {
          if (!rows || rows.length === 0) {
            return interaction.reply({ content: "🧾 Sem histórico ainda.", ephemeral: true });
          }

          const txt = rows
            .map((r) => `• **${r.status}** | ${String(r.tipo).toUpperCase()} | ${r.quantidade} | ${r.data}`)
            .join("\n");

          return interaction.reply({ content: `🧾 **Últimos registros:**\n\n${txt}`, ephemeral: true });
        }
      );

      return;
    }

    if (escolha === "ver_devedores") {
      if (!isGerente && !is00) return interaction.reply({ content: "Sem permissão.", ephemeral: true });

      db.all(`SELECT * FROM usuarios WHERE divida > 0 ORDER BY divida DESC`, [], (err, rows) => {
        if (!rows || rows.length === 0) return interaction.reply({ content: "✅ Ninguém está devendo.", ephemeral: true });

        const lista = rows.map((u) => `• <@${u.id}> — 💰 **${u.divida}**`).join("\n");
        return interaction.reply({ content: `📋 **Devedores:**\n\n${lista}`, ephemeral: true });
      });

      return;
    }

    if (escolha === "alterar_saldo") {
      if (!is00) return interaction.reply({ content: "Apenas cargo 00.", ephemeral: true });

      return interaction.reply({
        content:
          "⚙️ **Alterar saldo (somente 00)**\n\n" +
          "Use:\n" +
          "`!editar @usuario +50`\n" +
          "`!editar @usuario -50`\n",
        ephemeral: true,
      });
    }

    return;
  }

  // ==========================
  // 🔥 BOTÕES APROVAR / NEGAR
  // ==========================
  if (!interaction.isButton()) return;

  if (!isGerente && !is00) {
    return interaction.reply({ content: "❌ Apenas Gerentes/00 podem usar.", ephemeral: true });
  }

  const msgId = interaction.message.id;

  await interaction.deferUpdate();

  // ✅ Anti-dupla persistente no DB
  db.get(
    `SELECT id FROM historico WHERE msgId = ? AND (status = 'APROVADO' OR status = 'NEGADO') LIMIT 1`,
    [msgId],
    async (err, already) => {
      if (already) {
        return interaction.followUp({ content: "⚠️ Esse farme já foi processado.", ephemeral: true });
      }

      const [acao, userId, quantidadeStr, tipo] = interaction.customId.split("_");
      const quantidade = parseInt(quantidadeStr, 10);

      if (!["aprovar", "negar"].includes(acao)) return;

      const dataAgora = nowBR();
      const hojeDia = todayKey();

      // ✅ APROVAR
      if (acao === "aprovar") {
        db.get(`SELECT * FROM usuarios WHERE id = ?`, [userId], async (err2, row) => {
          if (!row) {
            db.run(`INSERT INTO usuarios VALUES (?, ?, ?, ?)`, [userId, hojeDia, 0, 0]);
            row = { ultimoDia: hojeDia, entregueHoje: 0, divida: 0 };
          }

          let entregueHoje = row.entregueHoje;
          let divida = row.divida;
          let ultimoDia = row.ultimoDia;

          if (ultimoDia !== hojeDia) {
            const falta = 100 - entregueHoje;
            if (falta > 0) divida += falta;
            entregueHoje = 0;
            ultimoDia = hojeDia;
          }

          entregueHoje += quantidade;

          db.run(
            `UPDATE usuarios SET ultimoDia = ?, entregueHoje = ?, divida = ? WHERE id = ?`,
            [ultimoDia, entregueHoje, divida, userId]
          );

          db.run(
            `INSERT INTO historico (userId, tipo, quantidade, status, data, msgId, gerenteId)
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [userId, tipo, quantidade, "APROVADO", dataAgora, msgId, interaction.user.id]
          );

          await interaction.message.edit({ content: "✅ Farme aprovado!", components: [] });

          const user = await client.users.fetch(userId).catch(() => null);
          if (user) await safeDM(user, `✅ Seu farme de **${quantidade} ${tipo}** foi **APROVADO**!`);

          if (logChannel) {
            const embed = new EmbedBuilder()
              .setTitle("✅ FARME APROVADO")
              .setColor("Green")
              .addFields(
                { name: "👤 Membro", value: `<@${userId}>`, inline: true },
                { name: "👮 Gerente", value: `<@${interaction.user.id}>`, inline: true },
                { name: "📦 Tipo", value: tipo.toUpperCase(), inline: true },
                { name: "🔢 Quantidade", value: `${quantidade}`, inline: true },
                { name: "📊 Entregue Hoje", value: `${entregueHoje}/100`, inline: true },
                { name: "💰 Dívida", value: `${divida}`, inline: true }
              )
              .setFooter({ text: `ID: ${msgId}` })
              .setTimestamp();

            logChannel.send({ embeds: [embed] }).catch(() => null);
          }
        });

        return;
      }

      // ❌ NEGAR
      if (acao === "negar") {
        db.run(
          `INSERT INTO historico (userId, tipo, quantidade, status, data, msgId, gerenteId)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          [userId, tipo, quantidade, "NEGADO", dataAgora, msgId, interaction.user.id]
        );

        await interaction.message.edit({ content: "❌ Farme negado!", components: [] });

        const user = await client.users.fetch(userId).catch(() => null);
        if (user) await safeDM(user, `❌ Seu farme de **${quantidade} ${tipo}** foi **NEGADO**.`);

        if (logChannel) {
          const embed = new EmbedBuilder()
            .setTitle("❌ FARME NEGADO")
            .setColor("Red")
            .addFields(
              { name: "👤 Membro", value: `<@${userId}>`, inline: true },
              { name: "👮 Gerente", value: `<@${interaction.user.id}>`, inline: true },
              { name: "📦 Tipo", value: tipo.toUpperCase(), inline: true },
              { name: "🔢 Quantidade", value: `${quantidade}`, inline: true }
            )
            .setFooter({ text: `ID: ${msgId}` })
            .setTimestamp();

          logChannel.send({ embeds: [embed] }).catch(() => null);
        }

        return;
      }
    }
  );
});

// ==========================
// 🔐 START (somente depois do DB OK)
// ==========================
(async () => {
  try {
    await initDB();
    if (!process.env.DISCORD_TOKEN) {
      console.error("Faltando DISCORD_TOKEN nas env vars do Render!");
      process.exit(1);
    }
    client.login(process.env.DISCORD_TOKEN);
  } catch (e) {
    console.error("Falha ao iniciar:", e);
    process.exit(1);
  }
})();