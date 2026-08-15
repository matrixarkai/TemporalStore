const header = document.querySelector(".site-header");

const updateHeader = () => {
  if (window.scrollY > 12) {
    header.classList.add("is-scrolled");
  } else {
    header.classList.remove("is-scrolled");
  }
};

updateHeader();
window.addEventListener("scroll", updateHeader, { passive: true });

const productMenus = document.querySelectorAll(".nav-product-menu");

productMenus.forEach((menu) => {
  const trigger = menu.querySelector(".nav-product-trigger");
  if (!trigger) return;

  trigger.addEventListener("click", () => {
    const willOpen = !menu.classList.contains("is-open");
    productMenus.forEach((otherMenu) => {
      otherMenu.classList.remove("is-open");
      otherMenu.querySelector(".nav-product-trigger")?.setAttribute("aria-expanded", "false");
    });
    menu.classList.toggle("is-open", willOpen);
    trigger.setAttribute("aria-expanded", String(willOpen));
  });
});

document.addEventListener("click", (event) => {
  if (event.target.closest(".nav-product-menu")) return;
  productMenus.forEach((menu) => {
    menu.classList.remove("is-open");
    menu.querySelector(".nav-product-trigger")?.setAttribute("aria-expanded", "false");
  });
});

document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  productMenus.forEach((menu) => {
    menu.classList.remove("is-open");
    menu.querySelector(".nav-product-trigger")?.setAttribute("aria-expanded", "false");
  });
});


const apiKeyPayload = document.querySelector("#api-key-payload");
const apiKeyForm = document.querySelector(".access-form");

const updateApiKeyPayload = () => {
  if (!apiKeyPayload || !apiKeyForm) return;
  const checkedScopes = Array.from(apiKeyForm.querySelectorAll('input[type="checkbox"]:checked')).map((item) => item.value);
  const scopeProfiles = {
    agent_hook: ["context:ingest", "context:retrieve", "context:feedback"],
    session_batch_extractor: ["context:ingest", "context:replay"],
    resource_skill_ingestor: ["resource:ingest", "context:ingest"],
    tenant_admin: ["admin:account", "admin:user", "admin:api_key", "admin:audit"],
  };
  const role = document.querySelector("#request-role")?.value || "agent_service";
  const recommendedScopes = scopeProfiles[role] || checkedScopes;
  const payload = {
    account_id: document.querySelector("#request-account")?.value || "acct_acme",
    tenant_id: document.querySelector("#request-tenant")?.value || "tenant_prod",
    role,
    display_name: `${role} key`,
    scopes: recommendedScopes,
    allowed_user_ids: [document.querySelector("#request-user")?.value || "alice"].filter(Boolean),
    allowed_session_ids: [document.querySelector("#request-session")?.value || ""].filter(Boolean),
    expires_at_ms: 4102444800000,
  };
  apiKeyPayload.textContent = JSON.stringify(payload, null, 2);
};

apiKeyForm?.addEventListener("input", updateApiKeyPayload);
updateApiKeyPayload();


// --- Account access: Gmail auto-login, GitHub, and email sign in / register ---
const authForm = document.querySelector("#auth-form");
const authPayload = document.querySelector("#auth-payload");
if (authForm && authPayload) {
  const authCard = document.querySelector(".auth-card");
  const value = (selector, fallback) => (document.querySelector(selector)?.value || fallback || "").trim();
  const registerScopes = [
    "admin:account", "admin:user", "admin:api_key", "admin:sso", "admin:audit",
    "portal:read", "context:ingest", "context:retrieve", "context:feedback", "context:replay", "resource:read", "skill:read",
  ];
  let authMode = "signin";
  let authProvider = "password";

  const buildAuthPayload = () => {
    const email = value("#auth-email", "alice@gmail.com");
    const account_id = value("#auth-account", "acct_acme");
    const tenant_id = value("#auth-tenant", "tenant_prod");
    if (authProvider === "google") {
      return { endpoint: "/api/auth/sso_callback", tool: "matrixark_auth_sso_callback",
        arguments: { provider: "google", id_token: "<google-oidc-id-token>", google_client_id: "<your-google-client-id>", account_id, tenant_id } };
    }
    if (authProvider === "github") {
      return { endpoint: "/api/auth/sso_callback", tool: "matrixark_auth_sso_callback",
        arguments: { provider: "github", external_user_id: email, email, trusted_gateway: true, account_id, tenant_id } };
    }
    if (authMode === "register") {
      return { endpoint: "/api/auth/signup", tool: "matrixark_auth_signup",
        arguments: { provider: "password", email, password: "••••••••", display_name: value("#auth-name", "Alice Chen"), account_id, tenant_id, user_id: "usr_" + (email.split("@")[0] || "user"), first_key_scopes: registerScopes } };
    }
    return { endpoint: "/api/auth/login", tool: "matrixark_auth_login",
      arguments: { email, password: "••••••••", account_id, tenant_id, provider: "password" } };
  };

  const renderAuth = () => {
    authPayload.textContent = JSON.stringify(buildAuthPayload(), null, 2);
    const title = document.querySelector("#auth-payload-title");
    if (title) {
      title.textContent = authProvider === "google" ? "Google verified → sso_callback"
        : authProvider === "github" ? "GitHub → sso_callback"
        : authMode === "register" ? "Register → signup" : "Sign in → login";
    }
  };

  const setAuthMode = (mode) => {
    authMode = mode;
    authProvider = "password";
    authCard?.classList.toggle("is-register", mode === "register");
    document.querySelectorAll(".auth-tab").forEach((tab) => {
      const on = tab.dataset.authMode === mode;
      tab.classList.toggle("is-on", on);
      tab.setAttribute("aria-selected", String(on));
    });
    const submit = document.querySelector("#auth-submit");
    if (submit) submit.textContent = mode === "register" ? "Create account" : "Sign in";
    renderAuth();
  };

  document.querySelectorAll(".auth-tab").forEach((tab) => tab.addEventListener("click", () => setAuthMode(tab.dataset.authMode)));
  document.querySelectorAll("[data-auth-provider]").forEach((btn) => btn.addEventListener("click", () => { authProvider = btn.dataset.authProvider; renderAuth(); }));
  authForm.addEventListener("input", () => { authProvider = "password"; renderAuth(); });
  authForm.addEventListener("submit", (event) => { event.preventDefault(); authProvider = "password"; renderAuth(); });
  setAuthMode("signin");
}
