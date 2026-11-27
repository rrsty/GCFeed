import http from "k6/http";
import { check, sleep } from "k6";
import { Rate } from "k6/metrics";

const BASE_URL = (__ENV.BASE_URL || "http://127.0.0.1:18088").replace(/\/$/, "");
const VIDEO_ID = Number(__ENV.VIDEO_ID || 49990);
const USER_COUNT = Number(__ENV.USER_COUNT || 100);
const THINK_TIME = Number(__ENV.THINK_TIME !== undefined ? __ENV.THINK_TIME : 0);
const PASSWORD = __ENV.PASSWORD || "123456";

const likeSuccessRate = new Rate("like_success_rate");

export const options = {
  vus: Number(__ENV.VUS || 50),
  duration: __ENV.DURATION || "20s",
  thresholds: {
    http_req_failed: ["rate<0.05"],
    like_success_rate: ["rate>0.95"],
  },
};

export function setup() {
  const tokens = [];
  for (let i = 1; i <= USER_COUNT; i++) {
    const res = http.post(
      `${BASE_URL}/api/sessions`,
      JSON.stringify({ account: `test_user_${i}`, password: PASSWORD }),
      { headers: { "Content-Type": "application/json" } }
    );
    if (res.status !== 200) {
      throw new Error(`login failed for test_user_${i}: status=${res.status} body=${res.body}`);
    }
    tokens.push(res.json().access_token);
  }
  return { tokens };
}

export default function (data) {
  const token = data.tokens[(__VU - 1) % data.tokens.length];
  const isLike = __ITER % 2 === 0;
  const method = isLike ? "PUT" : "DELETE";
  const url = `${BASE_URL}/api/videos/${VIDEO_ID}/like`;
  const params = {
    headers: {
      Authorization: `Bearer ${token}`,
      "Idempotency-Key": `k6-${__VU}-${__ITER}`,
    },
  };

  const res = http.request(method, url, null, params);
  const ok = check(res, {
    "status is 200": (r) => r.status === 200,
    "has like_count": (r) => {
      try {
        const body = r.json();
        return typeof body.like_count === "number";
      } catch (e) {
        return false;
      }
    },
  });
  likeSuccessRate.add(ok);

  if (THINK_TIME > 0) {
    sleep(THINK_TIME);
  }
}
