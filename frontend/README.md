# Frontend

Static HTML/CSS/JavaScript dashboard for Theme Park Hall of Shame.

## Quick Start

```bash
# Serve locally with Python
python3 -m http.server 8000

# Or with Node
npx serve .
```

Visit http://localhost:8000

## Project Structure

```
frontend/
├── index.html         # Main dashboard page
├── css/
│   └── styles.css     # All styles
├── js/
│   ├── config.js      # API configuration
│   ├── api-client.js  # Backend API wrapper
│   ├── main.js        # App initialization
│   └── ...            # View-specific modules
└── assets/            # Images, fonts
```

## Configuration

Edit `js/config.js` to set the API URL:

```javascript
const CONFIG = {
    API_BASE_URL: '/api',  // Same domain
    // or
    API_BASE_URL: 'https://api.yoursite.com/api',  // Different domain
};
```

## Views

- **Park Rankings** - Parks sorted by downtime
- **Ride Performance** - Individual ride reliability
- **Wait Times** - Current wait time data
- **Trends** - Performance over time

## No Build Step

This is a vanilla HTML/CSS/JS project with no framework or build process. Just deploy the files to any static hosting service.

## Deployment Options

- Netlify (see `netlify.toml`)
- Vercel (see `vercel.json`)
- AWS S3 + CloudFront
- GitHub Pages
- Any static web server

See [docs/deployment.md](../docs/deployment.md) for detailed instructions.
