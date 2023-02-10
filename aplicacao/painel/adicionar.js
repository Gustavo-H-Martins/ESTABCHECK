document.getElementById("add-cnpj-form").addEventListener("submit", function (event) {
event.preventDefault();

const formData = {};
const formElements = document.getElementById("add-cnpj-form").elements;
for (let i = 0; i < formElements.length; i++) {
    const input = formElements[i];
    if (input.name) {
    //alert(input.name);
    formData[input.name] = input.value;
    }
}

console.log(`Dados incluídos
${JSON.stringify(formData)}`)
alert(`Dados de ${formElements[0].value} incluídos com sucesso!`)
const request = new XMLHttpRequest();
request.open("POST", "http://localhost:8000/estabelecimentos/insert/cnpj", true);
request.setRequestHeader("Content-Type", "application/json");
request.send(JSON.stringify(formData));
});

/*
const inputCnpj = document.querySelector("input[name='cnpj']");
inputCnpj.addEventListener("change", (event) => {
const cnpj = event.target.value.replace(/[^0-9]/g, '');
fetch(`http://localhost:8000/estabelecimentos/get/cnpj=${cnpj}`)
    .then((response) => response.json())
    .then((data) => {
    if (!data.RAZAO_SOCIAL) {
        data.RAZAO_SOCIAL = '';
    }
    document.querySelector("input[name='razao_social']").value = data.RAZAO_SOCIAL;
    document.querySelector("input[name='nome_fantasia']").value = data.NOME_FANTASIA;
    document.querySelector("input[name='rua']").value = data.RUA;
    document.querySelector("input[name='numero']").value = data.NUMERO;
    document.querySelector("input[name='complemento']").value = data.COMPLEMENTO;
    document.querySelector("input[name='bairro']").value = data.BAIRRO;
    document.querySelector("input[name='cidade']").value = data.CIDADE;
    document.querySelector("input[name='estado']").value = data.ESTADO;
    document.querySelector("input[name='cep']").value = data.CEP;
    document.querySelector("input[name='latitude']").value = data.LATITUDE;
    document.querySelector("input[name='longitude']").value = data.LONGITUDE;
    document.querySelector("input[name='telefone1']").value = data.TELEFONE1;
    document.querySelector("input[name='site']").value = data.SITE;
    document.querySelector("input[name='cnae_descricao']").value = data.CNAE_DESCRICAO;
    document.querySelector("input[name='horario_funcionamento']").value = data.HORARIO_FUNCIONAMENTO;
    document.querySelector("input[name='instagram']").value = data.INSTAGRAM;
    document.querySelector("input[name='facebook']").value = data.FACEBOOK;
    document.querySelector("input[name='opcoes_de_servico']").value = data.OPCOES_DE_SERVICO;
});
});
*/

const inputCnpj = document.querySelector("input[name='cnpj']");
inputCnpj.addEventListener("change", (event) => {
const cnpj = event.target.value;
fetch(`http://localhost:8000/estabelecimentos/get/cnpj=${cnpj.replace(/[^0-9]/g, '')}`)
.then((response) =>
    response.json())
.then((data) => {
    const formElements = document.getElementById("add-cnpj-form").elements;
    for (let i = 1; i < formElements.length; i++) {
    const nome = formElements[i].name;
    const valor = formElements[i].value
    //alert(document.querySelector(`input[name='${nome}']`))
    const value = data[nome.toUpperCase()];
    document.querySelector(`input[name='${nome}']`).value = value || "";
    }
});
fetch(`http://localhost:8000/estabelecimentos/get/cnpj=${cnpj.replace(/[^0-9]/g, '')}`)
.then((response) => {
if (response.status === 200) {
    swal({
        title: `CNPJ: ${cnpj} já existe na base!`,
        text: "Clique no botão para ser redirecionado para página de alteração!",
        icon: "warning",
        buttons: true,
      }).then(function(result) {
        if (result) {
          window.location.href='./editar.html';
        } else {
          console.log("Operação cancelada ");
        }
      });
} else {
    console.log(`CNPJ: ${cnpj} não encontrado!`)
}
});
});

const form_cnpj = document.querySelector("input[name='cnpj']");
      form_cnpj.addEventListener("change", (event) => {
          document.querySelector("input[name='cnpj']").value = document.querySelector("input[name='cnpj']").value.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5")
          });