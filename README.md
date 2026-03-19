# blue-marine-tracker
Tracking weather and location as my dad sails acro

## Production deployment

For production, put a Cloudflare proxy in front of the Railway domain to eliminate egress costs. Cloudflare's free plan caches static assets and has unlimited bandwidth. All uploaded media is served with `Cache-Control: public, max-age=604800, immutable` headers, ETags, and Last-Modified headers, so Cloudflare will cache them automatically and only hit your origin once per file.
