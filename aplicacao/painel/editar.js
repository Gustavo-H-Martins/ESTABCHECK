document.getElementById("update-cnpj-form").addEventListener("submit", function (event) {
    event.preventDefault();
    
    const formData = {};
    const formElements = document.getElementById("update-cnpj-form").elements;
    for (let i = 0; i < formElements.length; i++) {
        const input = formElements[i];
        if (input.name) {
        //alert(input.name);
        formData[input.name] = input.value;
        }
    }
    console.log(`Dados atualizados
    ${JSON.stringify(formData)}`)
    alert(`Dados de ${formElements[0].value} atualizados com sucesso!`)
    const request = new XMLHttpRequest();
    request.open("PUT", `http://localhost:8000/estabelecimentos/update/cnpj=${formElements[0].value.replace(/[^0-9]/g, '')}`, true);
    request.setRequestHeader("Content-Type", "application/json");
    request.onload = () => {
        if (request.status === 200) {
            console.log(`Dados do estabelecimento ${formElements[1].value} atualizado com sucesso`);
        } else { 
            console.error(`Erro ao atualizar dados de ${formElements[0].value}`);
        }
    };
    request.send(JSON.stringify(formData));
    });
    
    const inputCnpj = document.querySelector("input[name='cnpj']");
    inputCnpj.addEventListener("change", (event) => {
    const cnpj = event.target.value;
    fetch(`http://localhost:8000/estabelecimentos/get/cnpj=${cnpj.replace(/[^0-9]/g, '')}`)
    .then((response) =>
        response.json())
    .then((data) => {
        const formElements = document.getElementById("update-cnpj-form").elements;
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
    } else {
        swal({
            title: `CNPJ: ${cnpj} não encontrado!`,
            text: "Clique no botão para ser redirecionado para página de inclusão!",
            icon: "warning",
            buttons: true,
          }).then(function(result) {
            if (result) {
              window.location.href='./adicionar.html';
            } else {
              console.log("Operação cancelada ");
            }
          });
    }
    });
    });
    
    const form_cnpj = document.querySelector("input[name='cnpj']");
          form_cnpj.addEventListener("change", (event) => {
              document.querySelector("input[name='cnpj']").value = document.querySelector("input[name='cnpj']").value.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5")
              });

            