import http from "k6/http";
import { check, sleep } from "k6";
import { Rate } from "k6/metrics";

// 配置参数（可通过命令行环境变量覆盖，例如 -e SCENE=hot -e VUS=50）
const BASE_URL = (__ENV.BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const SCENE = __ENV.SCENE || "timeline"; // 可选: timeline (最新流), hot (热门流), recommend (推荐流)
const LIMIT = Number(__ENV.LIMIT || 10);
const THINK_TIME = Number(__ENV.THINK_TIME !== undefined ? __ENV.THINK_TIME : 1); // 思考时间(秒)，极限压测设为 0
const ACCOUNT = __ENV.ACCOUNT || "test_user_1";
const PASSWORD = __ENV.PASSWORD || "123456";

const successRate = new Rate("feed_success_rate");

export const options = {
  vus: Number(__ENV.VUS || 20),
  duration: __ENV.DURATION || "30s",
  thresholds: {
    http_req_failed: ["rate<0.01"],     // HTTP 错误率小于 1%
    http_req_duration: ["p(95)<500"],   // 95% 请求在 500ms 内完成
    feed_success_rate: ["rate>0.99"],   // 业务成功率大于 99%
  },
};

// setup 在压测开始前执行一次（如果是推荐流，则登录获取 Token）
export function setup() {
  if (SCENE === "recommend") {
    const res = http.post(
      `${BASE_URL}/api/sessions`,
      JSON.stringify({ account: ACCOUNT, password: PASSWORD }),
      { headers: { "Content-Type": "application/json" } }
    );
    if (res.status !== 200) {
      throw new Error(`Login failed (status=${res.status}): ${res.body}`);
    }
    return { token: res.json().access_token };
  }
  return {};
}

export default function (data) {
  let res;

  if (SCENE === "recommend") {
    // 推荐流是 POST 请求
    res = http.post(
      `${BASE_URL}/api/feed-queries`,
      JSON.stringify({
        scene: "recommend",
        limit: LIMIT,
        context: { request_id: `k6-${Date.now()}-${__VU}-${__ITER}` },
      }),
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${data.token}`,
        },
      }
    );
  } else {
    // 最新流 (timeline) 或 热门流 (hot) 是 GET 请求
    res = http.get(`${BASE_URL}/api/feed-items?scene=${encodeURIComponent(SCENE)}&limit=${LIMIT}`);
  }

  const ok = check(res, {
    "status is 200": (r) => r.status === 200,
    "has items array": (r) => {
      try {
        const body = r.json();
        return Array.isArray(body.items);
      } catch (e) {
        return false;
      }
    },
  });

  successRate.add(ok);

  if (THINK_TIME > 0) {
    sleep(THINK_TIME);
  }
}
