import http from 'k6/http';
import { check } from 'k6';

export const options = {
    timeout: '10s',
    scenarios: {
        reads_scenario: {
            executor: 'constant-arrival-rate',
            rate: 40,
            maxVUs: 100,
            timeUnit: '1s',
            duration: '24h',
            startTime: '10s',
            preAllocatedVUs: 50,
            exec: 'readBalance',
        },
        // reads_scenario: {
        //     executor: 'ramping-arrival-rate',
        //     startRate: 5,
        //     timeUnit: '1s',
        //     preAllocatedVUs: 50,
        //     maxVUs: 1000,
        //     startTime: '20s',
        //
        //     stages: [
        //         { duration: '1m', target: 5 },
        //         { duration: '1m', target: 20 },
        //         { duration: '1m', target: 30 },
        //
        //         { duration: '1m', target: 40 },
        //
        //         { duration: '24h', target: 40 },
        //
        //         { duration: '30s', target: 0 },
        //     ],
        //     exec: 'readBalance',
        // },
        // orders_scenario: {
        //     executor: 'constant-arrival-rate',
        //     rate: 1000,
        //     maxVUs: 1500,
        //     preAllocatedVUs: 10,
        //     timeUnit: '1s',
        //     duration: '24h',
        //     startTime: '10s',
        //     exec: 'placeOrder',
        // },
        orders_scenario: {
            executor: 'ramping-arrival-rate',
            startRate: 10,
            timeUnit: '1s',
            preAllocatedVUs: 50,
            maxVUs: 500,
            // startTime: '10s',

            stages: [
                { duration: '30s', target: 10 },
                { duration: '30s', target: 100 },
                { duration: '1m', target: 500 },

                { duration: '1m', target: 1000 },

                { duration: '24h', target: 1000 },

                { duration: '30s', target: 0 },
            ],
            exec: 'placeOrder',
        },
    },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';

function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        let r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

function getRandomUserId() {
    return Math.floor(Math.random() * 100000) + 1;
}

export function readBalance() {
    const userId = getRandomUserId();

    const res = http.get(`${BASE_URL}/accounts/postings/user_${userId}/USD`);

    check(res, {
        'GET balance status is 200': (r) => r.status === 200,
    });
}

export function placeOrder() {
    const userId = getRandomUserId();

    const isBuy = Math.random() > 0.5;

    const payload = JSON.stringify({
        owner_id: `user_${userId}`,
        ticker: 'AAPL',
        side: isBuy ? 'BUY' : 'SELL',
        quantity: Math.floor(Math.random() * 10) + 1,
        price: 15000
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': uuidv4()
        },
    };

    const res = http.post(`${BASE_URL}/orders`, payload, params);

    check(res, {
        'POST order status is 200/201': (r) => r.status === 200 || r.status === 201,
    });
}