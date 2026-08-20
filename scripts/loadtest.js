import http from 'k6/http';
import { check } from 'k6';

export const options = {
    scenarios: {
        reads_scenario: {
            executor: 'constant-arrival-rate',
            rate: 10,
            timeUnit: '1s',
            duration: '24h',
            preAllocatedVUs: 50,
            maxVUs: 1000,
            exec: 'readBalance',
        },
        orders_scenario: {
            executor: 'constant-arrival-rate',
            rate: 1,
            timeUnit: '1s',
            duration: '24h',
            preAllocatedVUs: 10,
            maxVUs: 100,
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