function atualizaPainel(){
  var url = 'http://localhost:8000/estabelecimentos/get/all'
  fetch(url)
  .then(function (res) {
    return res.json();
  }).then(function (apiData) {
    renderizaDadosNaTabela(apiData, 50);

  });
}

function renderizaDadosNaTabela(dados, chunkSize) {
  
  const inicio = paginaAtual * tamanhoPagina;
  const fim = inicio + tamanhoPagina;
  const paginaDados = dados.slice(inicio, fim);

  const tabela = document.getElementById("corpo-tabela");
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
