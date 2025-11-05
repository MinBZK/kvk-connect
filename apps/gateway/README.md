# KVK API Gateway (Nginx)

We gebruiken een lokale proxy om alle requests naar de KVK API te sturen. Hiermee kunnen we voldoen aan de rate limiting
eisen van de KVK API.

KVK stelt standaard een rate limit van 100 request per seconden en 300k requests per maand.

De service geeft een 429 response code terug als de limiet overschreden wordt. Verwacht wordt dat de client API deze
429 core respecteert. De clientcode van de KVK Connector doet dit ook.

### Environment Variables

| Variable            | Default      | Beschrijving                 |
|---------------------|--------------|------------------------------|
| `RATE_LIMIT_CALLS`  | `100`        | Aantal requests/s toegestaan |
| `KVK_API_HOST`      | `api.kvk.nl` | KVK API hostname             |
