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
