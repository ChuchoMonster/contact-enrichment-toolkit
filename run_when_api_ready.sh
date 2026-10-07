#!/bin/bash
# Polls BlitzAPI every 5 minutes. When it responds, runs the v2 script.
cd "$(dirname "$0")" || exit 1

API_KEY=$(grep BLITZ_API_KEY .env | cut -d= -f2 | tr -d '"' | tr -d "'")

echo "Waiting for BlitzAPI to come back online..."
while true; do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
        -H "x-api-key: $API_KEY" -H "Content-Type: application/json" \
        -d '{"domain":"nike.com"}' \
        --max-time 15 \
        https://api.blitz-api.ai/api/search/domain-to-linkedin-company)

    if [ "$STATUS" = "200" ]; then
        echo "$(date): API is back! Starting v2 script..."
        python3 seo_contact_finder.py 2>&1 | tee seo_contacts_v2.log
        echo "$(date): v2 script finished!"
        break
    else
        echo "$(date): API returned $STATUS — retrying in 5 minutes..."
        sleep 300
    fi
done
