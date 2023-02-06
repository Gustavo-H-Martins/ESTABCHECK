function atualizaPainel(){
  var url = 'http://localhost:3000/estabelecimentos/get/all'
  fetch(url)
  .then(function (res) {
    return res.json();
  }).then(function (apiData) {
    //renderizaDadosNaTabela(apiData, 50);
    renderizarTabela(apiData)
  });
}

function renderizaDadosNaTabela(dados, chunkSize) {
  const tabela = document.getElementById("cnpjs");
  for (let i = 0; i < dados.length; i += chunkSize) {
    let chunk = dados.slice(i, i + chunkSize);
    chunk.forEach(dado => {
      let novaLinha = document.createElement("tr");
      Object.values(dado).forEach((value) => {
        let celula = document.createElement("td");
        celula.innerText = value;
        novaLinha.appendChild(celula);
      });
      tabela.appendChild(novaLinha);
    });
  }
}

const tabela = document.getElementById('cnpjs');
const corpoTabela = document.getElementById('corpo-tabela');
const anterior = document.getElementById('anterior');
const proximo = document.getElementById('proximo');

const tamanhoPagina = 50;
let paginaAtual = 0;

function renderizarTabela(dados){
  const corpoTabela = document.getElementById("cnpjs");
  corpoTabela.innerHTML = '';

  const inicio = paginaAtual * tamanhoPagina;
  const fim = inicio + tamanhoPagina;
  const paginaDados = dados.slice(inicio, fim);

  paginaDados.forEach((dado) => {
    const linha = document.createElement('tr');
    Object.values(dado).forEach((value) => {
      let celula = document.createElement('td');
      celula.innerText = value;
    });
    corpoTabela.appendChild(linha);
  });
}

anterior.addEventListener('click', () =>{
  if (paginaAtual > 0) {
    paginaAtual--;
    renderizarTabela(dados);
  }
});

proximo.addEventListener('click', () => {
  if (paginaAtual < Math.ceil(dados.length / tamanhoPagina) -1) {
    paginaAtual ++;
    renderizarTabela(dados);
  }
});

//renderizarTabela()