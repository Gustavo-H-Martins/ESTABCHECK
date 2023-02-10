// Adiciona dados na tabela
async function atualizaPainel() {
  var uf = document.getElementById('uf').value;
  var Select = document.getElementById("CNAE").selectedIndex;
  var Option = document.getElementById("CNAE").options;
  var cnae = Option[Select].value;
  //alert(cnae)
  try {
    const response = await fetch(`http://localhost:8000/estabelecimentos/get/uf=${uf}/numero_cnae=${cnae}`);
    const dados = await response.json();
    let data = dados.slice(0,200)
    // Aqui você pode manipular os dados como quiser

    const corpoTabela = document.getElementById("corpo-tabela");

    data.forEach((linha) => {
      const tr = document.createElement('tr');
      for (const key in linha) {
        let td = document.createElement("td");
        td.classList.add(key);
        td.innerText = linha[key];
        // Formata os CNPJ
        if (key === "CNPJ") {
          td.innerHTML = td.innerHTML.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5");
        }
        tr.appendChild(td);
      /*Object.values(linha).forEach((valor) => {
        const td = document.createElement('td');
        td.innerText = valor;
        tr.appendChild(td);
      });*/
    }
      corpoTabela.appendChild(tr);
    });
  } catch (error) {
    console.error(error);
  }
}


window.addEventListener('load', atualizaPainel())