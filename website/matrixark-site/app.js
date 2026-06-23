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
  const payload = {
    account_id: document.querySelector("#request-account")?.value || "acct_acme",
    tenant_id: document.querySelector("#request-tenant")?.value || "tenant_prod",
    role: document.querySelector("#request-role")?.value || "agent_service",
    display_name: `${document.querySelector("#request-role")?.value || "agent_service"} key`,
    scopes: checkedScopes,
    allowed_user_ids: [document.querySelector("#request-user")?.value || "alice"].filter(Boolean),
    allowed_session_ids: [document.querySelector("#request-session")?.value || ""].filter(Boolean),
    expires_at_ms: 4102444800000,
  };
  apiKeyPayload.textContent = JSON.stringify(payload, null, 2);
};

apiKeyForm?.addEventListener("input", updateApiKeyPayload);
updateApiKeyPayload();
