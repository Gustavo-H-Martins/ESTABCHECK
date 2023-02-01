const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bodyParser = require('body-parser');

const app = express();
const port = 3000;

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }))

// parse application/json
app.use(bodyParser.json())

// Connect to SQLite3 database
const db = new sqlite3.Database('../database/br_base_cnpj.db', (err) => {
  if (err) {
    console.error(err.message);
  }
  console.log('conectado no banco de dados "br_base_cnpj".');
});

// Get all data from the database
app.get('/cnpjs', (req, res) => {
  db.all('SELECT * FROM estabelecimentos LIMIT 20000', (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json({
      cnpjs: rows
    });
  });
});

// Get data by CNPJ
app.get('/cnpjs/:cnpj', (req, res) => {
  const cnpj = req.params.cnpj;
  db.get(`SELECT * FROM estabelecimentos WHERE CNPJ = ?`, [cnpj], (err, row) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    if (!row) {
      res.status(404).json({ error: `CNPJ ${cnpj} not found` });
      return;
    }
    res.json({
      cnpj: row
    });
  });
});

// Insert data into the database
app.post('/cnpjs', (req, res) => {
  const cnpj = req.body.cnpj;
  const razao_social = req.body.razao_social;
  const nome_fantasia = req.body.nome_fantasia;
  const rua = req.body.rua;
  const numero = req.body.numero;
  const complemento = req.body.complemento;
  const bairro = req.body.bairro;
  const cidade = req.body.cidade;
  const estado = req.body.estado;
  const cep = req.body.cep;
  const latitude = req.body.latitude;
  const longitude = req.body.longitude;
  const telefone1 = req.body.telefone1;
  const site = req.body.site;
  const cnae_descricao = req.body.cnae_descricao;
  const horario_funcionamento = req.body.horario_funcionamento;
  const instagram = req.body.instagram;
  const facebook = req.body.facebook;
  const opcoes_de_servico = req.body.opcoes_de_servico;
  
  db.run(`INSERT INTO estabelecimentos (CNPJ, RAZAO_SOCIAL, NOME_FANTASIA, RUA, NUMERO, COMPLEMENTO, BAIRRO, CIDADE, ESTADO, CEP, LATITUDE, LONGITUDE, TELEFONE1, SITE, CNAE_DESCRICAO, HORARIO_FUNCIONAMENTO, INSTAGRAM, FACEBOOK, OPCOES_DE_SERVICO) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [cnpj, razao_social, nome_fantasia, rua, numero, complemento, bairro, cidade, estado, cep, latitude, longitude, telefone1, site, cnae_descricao, horario_funcionamento, instagram, facebook, opcoes_de_servico], (err) => {
  if (err) {
  res.status(500).json({ error: err.message });
  return;
  }
  res.json({
  message: `CNPJ ${cnpj} adicionado com sucesso`,
  cnpj: cnpj
  });
  });
  });
  
  // Update data in the database
  app.put('/cnpjs/:cnpj', (req, res) => {
  const cnpj = req.params.cnpj;
  const razao_social = req.body.razao_social;
  const nome_fantasia = req.body.nome_fantasia;
  const rua = req.body.rua;
  const numero = req.body.numero;
  const complemento = req.body.complemento;
  const bairro = req.body.bairro;
  const cidade = req.body.cidade;
  const estado = req.body.estado;
  const cep = req.body.cep;
  const latitude = req.body.latitude;
  const longitude = req.body.longitude;
  const telefone1 = req.body.telefone1;
  const site = req.body.site;
  const cnae_descricao = req.body.cnae_descricao;
  const horario_funcionamento = req.body.horario_funcionamento;
  const instagram = req.body.instagram;
  const facebook = req.body.facebook;
  const opcoes_de_servico = req.body.opcoes_de_servico;
  
  db.run(`UPDATE estabelecimentos SET RAZAO_SOCIAL = ?, NOME_FANTASIA = ?, RUA = ?, NUMERO = ?, COMPLEMENTO = ?, BAIRRO = ?, CIDADE = ?, ESTADO = ?, CEP = ?, LATITUDE = ?, LONGITUDE = ?, TELEFONE1 = ?, SITE = ?, CNAE_DESCRICAO = ?, HORARIO_FUNCIONAMENTO = ?, INSTAGRAM = ?, FACEBOOK = ?, OPCOES_DE_SERVICO = ? WHERE CNPJ = ?`, 
  [razao_social, nome_fantasia, rua, numero, complemento, bairro, cidade, estado, cep, latitude, longitude, telefone1, site, cnae_descricao, horario_funcionamento, instagram, facebook, opcoes_de_servico, cnpj], (err) => { 
    if (err) { res.status(500).json({ error: err.message }); 
    return; } 
    res.json({ 
        message: `CNPJ ${cnpj} modificado com sucesso`
    });
});
});

// Delete data from the database
app.delete('/cnpjs/:cnpj', (req, res) => {
const cnpj = req.params.cnpj;

db.run(`DELETE FROM estabelecimentos WHERE CNPJ = ?`, cnpj, (err) => {
if (err) {
res.status(500).json({ error: err.message });
return;
}
res.json({
message: `CNPJ ${cnpj} deletado com sucesso`
});
});
});

// Start the server
app.listen(port, () => {
console.log(`Server rodando na porta:  ${port}`);
});