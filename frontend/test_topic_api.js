
const http = require('http');

const groupId = '88885251482412';
const options = {
    hostname: 'localhost',
    port: 8208,
    path: `/api/groups/${groupId}/stock/topics?page=1&per_page=10`,
    method: 'GET',
};

const req = http.request(options, (res) => {
    console.log(`STATUS: ${res.statusCode}`);
    console.log(`HEADERS: ${JSON.stringify(res.headers)}`);
    res.setEncoding('utf8');
    let data = '';
    res.on('data', (chunk) => {
        data += chunk;
    });
    res.on('end', () => {
        console.log('BODY:');
        try {
            const json = JSON.parse(data);
            console.log(JSON.stringify(json, null, 2));

            if (json.items && Array.isArray(json.items)) {
                console.log(`\nVerified: Received ${json.items.length} topics.`);
                if (json.items.length > 0) {
                    const first = json.items[0];
                    if (first.topic_id && first.mentions) {
                        console.log('Verified: Topic structure looks correct (has topic_id and mentions).');
                    } else {
                        console.error('FAILED: Topic structure missing keys.');
                    }
                }
            } else {
                console.error('FAILED: Invalid response structure (missing items array).');
            }

        } catch (e) {
            console.log(data);
            console.error('Failed to parse JSON:', e);
        }
    });
});

req.on('error', (e) => {
    console.error(`problem with request: ${e.message}`);
});

req.end();
