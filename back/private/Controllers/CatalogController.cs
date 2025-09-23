using Microsoft.AspNetCore.Mvc;

namespace DiscoData2API_Priv.Controllers
{
    [ApiController]
    [Route("catalog")]
    [ApiExplorerSettings(IgnoreApi = true)]
    public class CatalogController : ControllerBase
    {
        [HttpGet]
        public IActionResult Index()
        {
            var html = GenerateCatalogHtml();
            return Content(html, "text/html");
        }

        private string GenerateCatalogHtml()
        {
            return @"
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <meta name='viewport' content='width=device-width, initial-scale=1.0'>
    <title>Views Catalog Explorer</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #4a90e2 0%, #357abd 100%);
            color: white;
            padding: 15px;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }
        .header p {
            margin: 10px 0 0 0;
            opacity: 0.9;
        }
        .content {
            padding: 30px;
        }
        .view-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 20px;
        }
        .view-card {
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 20px;
            background: #fafafa;
            transition: all 0.3s ease;
            cursor: pointer;
        }
        .view-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            border-color: #4a90e2;
        }
        .view-name {
            font-size: 1.4em;
            font-weight: 600;
            color: #333;
            margin-bottom: 10px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .view-description {
            font-size: 0.9em;
            color: #666;
            margin-bottom: 15px;
            line-height: 1.4;
            display: -webkit-box;
            -webkit-line-clamp: 3;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        .view-meta {
            display: flex;
            justify-content: space-between;
            font-size: 0.8em;
            color: #888;
            margin-bottom: 10px;
        }
        .view-user {
            background: #e3f2fd;
            color: #1976d2;
            padding: 2px 6px;
            border-radius: 3px;
        }
        .view-date {
            color: #666;
        }
        .view-fields {
            font-size: 0.8em;
            color: #888;
            margin-top: 10px;
        }
        .loading {
            text-align: center;
            padding: 50px;
            color: #666;
        }
        .error {
            background: #ffebee;
            color: #c62828;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
        }
        .back-button {
            display: inline-block;
            margin-bottom: 20px;
            padding: 10px 20px;
            background: #4a90e2;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            transition: background-color 0.3s ease;
        }
        .back-button:hover {
            background: #357abd;
        }
        .filter-section {
            margin-bottom: 20px;
            padding: 15px;
            background: #f9f9f9;
            border-radius: 4px;
        }
        .filter-input {
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            margin-right: 10px;
            width: 200px;
        }
    </style>
</head>
<body>
    <div class='container'>
        <div class='header'>
            <h1>Views Catalog Explorer</h1>
            <p>Browse and explore your saved views and queries</p>
        </div>
        <div class='content'>
          
            <div class='filter-section'>
                <input type='text' id='userFilter' class='filter-input' placeholder='Filter by user...'>
                <input type='text' id='nameFilter' class='filter-input' placeholder='Filter by name...'>
                <input type='text' id='catalogFilter' class='filter-input' placeholder='Filter by catalog...'>
            </div>

            <div id='loading' class='loading'>Loading views...</div>
            <div id='error' class='error' style='display: none;'></div>
            <div id='views' class='view-grid' style='display: none;'></div>
        </div>
    </div>

    <script>
        let allViews = [];

        async function loadViews() {
            try {
                const response = await fetch('/api/View/GetCatalog');
                if (!response.ok) throw new Error('Failed to load views');

                allViews = await response.json();
                displayViews(allViews);
            } catch (error) {
                document.getElementById('loading').style.display = 'none';
                document.getElementById('error').style.display = 'block';
                document.getElementById('error').textContent = 'Error loading views: ' + error.message;
            }
        }

        function displayViews(views) {
            const container = document.getElementById('views');
            container.innerHTML = '';

            if (views.length === 0) {
                container.innerHTML = '<p style=""text-align: center; color: #666;"">No views found.</p>';
            } else {
                views.forEach(view => {
                    const card = document.createElement('div');
                    card.className = 'view-card';

                    const date = new Date(view.date).toLocaleDateString();
                    const fieldsCount = view.fields ? view.fields.length : 0;

                    // Truncate long names and descriptions
                    const truncateName = (name, maxLength = 50) => {
                        return name && name.length > maxLength ? name.substring(0, maxLength) + '...' : name;
                    };

                    const truncateDesc = (desc, maxLength = 120) => {
                        return desc && desc.length > maxLength ? desc.substring(0, maxLength) + '...' : desc;
                    };

                    card.innerHTML = `
                        <div class='view-name' title='${view.name || 'Unnamed View'}'>${truncateName(view.name) || 'Unnamed View'}</div>
                        <div class='view-description' title='${view.description || 'No description available'}'>${truncateDesc(view.description) || 'No description available'}</div>
                        <div class='view-meta'>
                            <span class='view-user'>${view.userAdded || 'Unknown'}</span>
                            <span class='view-date'>${date}</span>
                        </div>
                        <div class='view-fields'>${fieldsCount} field(s) | Version: ${view.version || 'N/A'} | Catalog: ${view.catalog || 'Default'}</div>
                    `;

                    card.addEventListener('click', () => {
                        const viewId = view.id || view.ID || view._id;
                        window.location.href = `/view.html?id=${encodeURIComponent(viewId)}`;
                    });

                    container.appendChild(card);
                });
            }

            document.getElementById('loading').style.display = 'none';
            document.getElementById('views').style.display = 'grid';
        }

        function filterViews() {
            const userFilter = document.getElementById('userFilter').value.toLowerCase();
            const nameFilter = document.getElementById('nameFilter').value.toLowerCase();
            const catalogFilter = document.getElementById('catalogFilter').value.toLowerCase();

            const filteredViews = allViews.filter(view => {
                const matchesUser = !userFilter || (view.userAdded && view.userAdded.toLowerCase().includes(userFilter));
                const matchesName = !nameFilter || (view.name && view.name.toLowerCase().includes(nameFilter));
                const matchesCatalog = !catalogFilter || (view.catalog && view.catalog.toLowerCase().includes(catalogFilter));
                return matchesUser && matchesName && matchesCatalog;
            });

            displayViews(filteredViews);
        }

        // Add event listeners for filtering
        document.getElementById('userFilter').addEventListener('input', filterViews);
        document.getElementById('nameFilter').addEventListener('input', filterViews);
        document.getElementById('catalogFilter').addEventListener('input', filterViews);

        // Load views on page load
        loadViews();
    </script>
</body>
</html>";
        }

    }
}