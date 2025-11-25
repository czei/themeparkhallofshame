# Frontend Deployment Guide

This guide covers deploying the Theme Park Hall of Shame frontend to various hosting platforms.

## Prerequisites

Before deploying, ensure you have:

1. **Backend API deployed and accessible**
   - The frontend needs to connect to the backend API
   - Note your backend API URL (e.g., `https://api.yoursite.com`)

2. **CORS configured on backend**
   - Backend must allow requests from your frontend domain
   - Update Flask CORS settings in `backend/src/api/app.py`

## Configuration

### Step 1: Update API URL

Edit `frontend/js/config.js` and set your production API URL:

```javascript
const CONFIG = {
    API_BASE_URL: 'https://your-backend-api.com/api',  // <-- Update this
    // ...
};
```

**Important**: Make sure the URL:
- Includes the full domain
- Ends with `/api` (the API route prefix)
- Uses `https://` for production

### Step 2: Test Locally

Before deploying, test that the frontend can connect to your backend:

```bash
# Serve the frontend locally
cd frontend
python3 -m http.server 8000

# Open in browser
# Visit: http://localhost:8000
# Check browser console for API connection errors
```

## Deployment Options

### Option 1: Netlify (Recommended for Static Sites)

**Automatic Deployment via Git:**

1. Create a Netlify account at https://netlify.com
2. Connect your GitHub repository
3. Configure build settings:
   - **Base directory**: `frontend`
   - **Build command**: (leave empty or `echo "No build needed"`)
   - **Publish directory**: `.` (current directory)
4. Deploy!

**Manual Deployment:**

```bash
# Install Netlify CLI
npm install -g netlify-cli

# Deploy from frontend directory
cd frontend
netlify deploy --prod
```

**Configuration**: The `netlify.toml` file is already configured with:
- SPA routing (all routes redirect to index.html)
- Security headers
- Cache control for static assets

### Option 2: Vercel

**Automatic Deployment via Git:**

1. Create a Vercel account at https://vercel.com
2. Import your GitHub repository
3. Configure project:
   - **Root Directory**: `frontend`
   - **Framework Preset**: Other
   - **Build Command**: (leave empty)
   - **Output Directory**: `.`
4. Deploy!

**Manual Deployment:**

```bash
# Install Vercel CLI
npm install -g vercel

# Deploy from frontend directory
cd frontend
vercel --prod
```

**Configuration**: The `vercel.json` file is already configured.

### Option 3: GitHub Pages

**Setup:**

1. Create a new branch for deployment:
```bash
git checkout -b gh-pages
```

2. Copy frontend files to root:
```bash
cp -r frontend/* .
git add .
git commit -m "Deploy frontend to GitHub Pages"
git push origin gh-pages
```

3. Enable GitHub Pages in repository settings:
   - Go to Settings > Pages
   - Source: Deploy from branch `gh-pages`
   - Folder: `/` (root)

**Important**: GitHub Pages serves from your repository root, so you need to copy frontend files to a separate branch.

### Option 4: AWS S3 + CloudFront

**Setup:**

1. Create S3 bucket:
```bash
aws s3 mb s3://themepark-hallofshame-frontend
```

2. Enable static website hosting:
```bash
aws s3 website s3://themepark-hallofshame-frontend \
  --index-document index.html \
  --error-document index.html
```

3. Upload files:
```bash
cd frontend
aws s3 sync . s3://themepark-hallofshame-frontend \
  --exclude ".git/*" \
  --exclude "*.md" \
  --exclude "*.toml" \
  --exclude "*.json"
```

4. Set up CloudFront distribution for HTTPS and global CDN

### Option 5: Any Static Host

The frontend is pure HTML/CSS/JavaScript with no build step. You can deploy to:

- **Cloudflare Pages**
- **Render**
- **Firebase Hosting**
- **DigitalOcean App Platform**

Just upload the `frontend` directory contents to any static hosting service!

## Backend CORS Configuration

Your backend needs to allow requests from your frontend domain. Update `backend/src/api/app.py`:

```python
from flask_cors import CORS

# Update CORS settings
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "https://your-frontend-site.netlify.app",  # Add your domain
            "http://localhost:8000",  # Keep for local dev
        ],
        "methods": ["GET", "OPTIONS"],
        "allow_headers": ["Content-Type"]
    }
})
```

## Post-Deployment Checklist

After deploying, verify:

- [ ] Frontend loads without errors
- [ ] Park Rankings view displays data
- [ ] Ride Performance view works
- [ ] Wait Times view works
- [ ] Park Details modal opens and loads data
- [ ] About modal opens
- [ ] Global filter (All Parks / Disney & Universal) works
- [ ] No CORS errors in browser console
- [ ] All links work (Queue-Times external links)
- [ ] Mobile responsive design works

## Troubleshooting

### Issue: "Failed to fetch" or CORS errors

**Solution**:
- Check that `API_BASE_URL` in `config.js` points to correct backend
- Verify backend CORS allows your frontend domain
- Check browser console for specific error messages

### Issue: API returns 404

**Solution**:
- Verify backend is running and accessible
- Check that backend URL includes `/api` prefix
- Test backend directly: `curl https://your-api.com/api/parks/downtime?period=today`

### Issue: Blank page after deployment

**Solution**:
- Check browser console for JavaScript errors
- Verify all script files loaded correctly (Network tab)
- Ensure `config.js` loads before `api-client.js`

### Issue: Styling looks broken

**Solution**:
- Verify `styles.css` loaded correctly
- Check that file paths are correct (no absolute paths)
- Clear browser cache

## Environment-Specific Configs

### Development
```javascript
// frontend/js/config.js
API_BASE_URL: '/api'  // Assumes backend on same origin (localhost:5001)
```

### Production with Same Domain
```javascript
// frontend/js/config.js
API_BASE_URL: '/api'  // Backend at yoursite.com/api, frontend at yoursite.com
```

### Production with Separate Domains
```javascript
// frontend/js/config.js
API_BASE_URL: 'https://api.themepark-shame.com/api'  // Full backend URL
```

## Monitoring

After deployment, monitor:

1. **Browser Console**: Check for JavaScript errors
2. **Network Tab**: Verify API requests succeed
3. **Performance**: Page load times should be fast (static site)
4. **Analytics** (optional): Add Google Analytics or similar

## Updates and Redeployment

To update the frontend:

1. Make changes locally
2. Test changes with local backend
3. Commit to git
4. Push to main branch
5. Hosting platform auto-deploys (if connected to Git)

Or for manual deployments:
```bash
cd frontend
netlify deploy --prod
# or
vercel --prod
```

## Security Notes

- All sensitive configuration (API keys, etc.) should be on the backend only
- Frontend code is public - never include secrets
- HTTPS is strongly recommended for production
- Security headers are configured in `netlify.toml` and `vercel.json`

## Support

For issues:
- Check browser console logs
- Test backend API directly
- Verify CORS configuration
- Review hosting platform logs
