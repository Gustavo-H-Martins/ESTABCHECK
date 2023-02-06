async function getDataAndUpdatePage() {
    const response = await getCNPJs();
    const cnpjs = response.data.cnpjs;
  
    // Clear any existing data in the HTML
    document.querySelector('#cnpj-list').innerHTML = '';
  
    // Add the new data to the HTML
    cnpjs.forEach(cnpj => {
      const newItem = document.createElement('li');
      newItem.textContent = cnpj;
      document.querySelector('#cnpj-list').appendChild(newItem);
    });
  }
  
  // Call the function to get the data and update the page
  getDataAndUpdatePage();