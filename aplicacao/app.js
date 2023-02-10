const fs = require('fs');
const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bodyParser = require('body-parser');
const cors  =require("cors");
const corsOptions ={
   origin:'*', 
   credentials:true,            //access-control-allow-credentials:true
   optionSuccessStatus:200,
}


const app = express();
const PORTA = 8000;

// parse aplicativo/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }))

// parse application/json
app.use(bodyParser.json());

// permitir acesso a API com CORS
app.use(cors(corsOptions)) ;
// Conectar ao banco de dados SQLite3
const db = new sqlite3.Database('../database/br_base_cnpj.db', (err) => {
  if (err) {
    console.error(err.message);
  }
  console.log('conectado no banco de dados "br_base_cnpj".');
});
// home da api
app.get('/estabelecimentos', (req, res) =>{
  retorno = `Bem vindo a api de acesso aos dados de estabelecimentos 
  Neste ambiente temos acesso aos seguintes dados de GET (Delete, Update e Create não serão expostos aqui) 
  http://${ip.address()}:${PORTA}/estabelecimentos/get/all 
  Retorna todos os dados de estabelecimentos na base (cuidado pra sua máquina não arriar)
  
  http://${ip.address()}:${PORTA}/estabelecimentos/get/cnpj={cnpj}
  Retorna dados de um estabelecimento em específico passando o parâmetro cnpj sem caracteres especiais (somente números)
  
  'http://${ip.address()}:${PORTA}/estabelecimentos/get/uf={uf}'
  Retorna todos os estabelecimentos dentro da uf informada
  `
  res.status(200).send(
    retorno
  )
});
/*
  MÉTODOS GET
*/
// Obter todos os dados de CNPJ
app.get('/estabelecimentos/get/all', (req, res) => {
  db.all('SELECT * FROM estabelecimentos LIMIT 2000', (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(
      rows
    );
  });
});

// Obter os dados por CNPJ
app.get('/estabelecimentos/get/cnpj=:cnpj', (req, res) => {
  const cnpj = req.params.cnpj;
  db.get(`SELECT * FROM estabelecimentos WHERE CNPJ = ?`, [cnpj], (err, row) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    if (!row) {
      res.status(404).json({ error: `CNPJ ${cnpj} não encontrado na base` });
      return;
    }
    res.json(
      row
    );
  });
});

// Obter dados pelo estado
app.get('/estabelecimentos/get/uf=:uf', (req, res) =>{
  var uf = req.params.uf.toUpperCase();
  db.all(`SELECT * FROM estabelecimentos WHERE ESTADO = ?`, [uf], (err, rows) =>{
    if(err) {
      res.status(500).json({error: err.message});
      return;
    }
    console.log(`Retornando ${rows.length} dados do ${uf}`)
    res.json(
      rows
    );
  });
});

// Obter dados pelo cnae e estado
app.get('/estabelecimentos/get/uf=:uf/numero_cnae=:numero_cnae', (req, res) => {
  var uf = req.params.uf.toUpperCase();
  var numero_cnae = parseInt(req.params.numero_cnae);
  //console.log(numero_cnae)
  //console.log(typeof numero_cnae)
  if (numero_cnae === 5611201) {
    cnae_descricao = 'Restaurantes e similares';
  } else if (numero_cnae === 5611203) {
    cnae_descricao = 'Lanchonetes casas de chá de sucos e similares';
  } else if (numero_cnae === 5611204){
    cnae_descricao = 'Bares e outros estabelecimentos especializados em servir bebidas sem entretenimento';
  } else if (numero_cnae === 5611205) {
    cnae_descricao = 'Bares e outros estabelecimentos especializados em servir bebidas com entretenimento';
  } else if (numero_cnae === 5612100) {
    cnae_descricao = 'Serviços ambulantes de alimentação';
  }
  //console.log(cnae_descricao)
  db.all(`SELECT * FROM estabelecimentos WHERE ESTADO = $1 AND CNAE_DESCRICAO = $2`, [uf, cnae_descricao], (err, rows) =>{
    if(err) {
      res.status(500).json({error: err.message});
      return;
    }
    console.log(`Retornando ${rows.length} dados do ${uf} cnae ${cnae_descricao, numero_cnae}`)
    res.json(
      rows
    );
  });
});

/*
  MÉTODOS POST
*/
// Insere dados no banco de dados CNPJ
app.post('/estabelecimentos/insert/cnpj', (req, res) => {
  const cnpj = req.body.cnpj.replace(/[^0-9]/g, '');
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
  
  // Update em um dado no banco CNPJ
  app.put('/estabelecimentos/update/cnpj=:cnpj', (req, res) => {
  const cnpj = req.params.cnpj.replace(/[^0-9]/g, '');
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

// Deleta um dado no Banco CNPJ
app.delete('/estabelecimentos/delete/cnpj=:cnpj', (req, res) => {
const cnpj = req.params.cnpj;
const usuario = req.body.usuario;
const chave_acesso = req.body.chave_acesso;

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

// Pega o ip do servidor
var ip = require("ip");
const { Console } = require('console');
const { type } = require('os');
//console.dir ( `O Servidor tem o seguinte IP Público Local ${ip.address()} `);

// Inicia o servidor

app.listen(PORTA, () => {
    console.log(`O servidor está escutando na porta ${PORTA} url: http://${ip.address()}:${PORTA}/estabelecimentos/` );
});