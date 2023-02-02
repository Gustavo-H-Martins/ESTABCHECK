// GET all data from the database
async function getCNPJs() {
    try {
      const response = await axios.get('http://localhost:3000/estabelecimentos/get/all');
      console.log(response.data.cnpjs);
    } catch (error) {
      console.error(error);
    }
  }
  
  // GET data by CNPJ
  async function getCNPJ(cnpj) {
    try {
      const response = await axios.get(`http://localhost:3000/estabelecimentos/get/cnpj=${cnpj}`);
      console.log(response.data.cnpj);
    } catch (error) {
      console.error(error);
    }
  }
  
  // POST data to the database
  async function addCNPJ(cnpjData) {
    try {
      const response = await axios.post('http://localhost:3000/estabelecimentos/insert/cnpj', cnpjData);
      console.log(response.data.message);
    } catch (error) {
      console.error(error);
    }
  }
  
  // PUT data to the database
  async function updateCNPJ(cnpj, cnpjData) {
    try {
      const response = await axios.put(`http://localhost:3000/estabelecimentos/update/cnpj=${cnpj}`, cnpjData);
      console.log(response.data.message);
    } catch (error) {
      console.error(error);
    }
  }
  
  // DELETE data from the database
  async function deleteCNPJ(cnpj) {
    try {
      const response = await axios.delete(`http://localhost:3000/estabelecimentos/delete/cnpj=${cnpj}`);
      console.log(response.data.message);
    } catch (error) {
      console.error(error);
    }
  }
  