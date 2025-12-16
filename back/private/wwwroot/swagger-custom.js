// Wait for Swagger UI to fully load
window.addEventListener('load', function() {
    // Add a slight delay to ensure Swagger UI is completely rendered
    setTimeout(function() {
        addCatalogButton();
    }, 1000);
});

function addCatalogButton() {
    // Check if button already exists
    if (document.querySelector('.catalog-button')) {
        return;
    }

    // Find the main wrapper or the operations section to place button above the API content
    const operationsSection = document.querySelector('.swagger-ui .opblock-tag-section') ||
                             document.querySelector('.swagger-ui .operations') ||
                             document.querySelector('.swagger-ui .wrapper');

    if (!operationsSection) {
        // Try again later if the element isn't found yet
        setTimeout(addCatalogButton, 500);
        return;
    }

    // Create a container div for the button
    const buttonContainer = document.createElement('div');
    buttonContainer.style.cssText = 'text-align: left; margin: 20px 0; padding: 10px;';

    // Create the catalog button
    const button = document.createElement('a');
    button.href = '/catalog';
    button.target = '_blank';
    button.className = 'catalog-button';
    button.innerHTML = 'Explore View Catalog';
    button.title = 'Browse available views and queries';

    buttonContainer.appendChild(button);

    // Insert the button container above the main content
    operationsSection.parentNode.insertBefore(buttonContainer, operationsSection);
}

// Also try to add the button when the DOM is ready (backup)
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(addCatalogButton, 2000);
});