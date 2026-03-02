"""
Workspace routing — auto-detect whether a contact belongs to the US or MX workspace.

Priority order (first match wins):
1. company_country == "Mexico" (case-insensitive) → MX
2. domain TLD ends in .mx → MX
3. website TLD ends in .mx → MX
4. Spanish business suffix in company name → MX
5. company_city or company_state in known Mexican cities/states list → MX
6. Default → US
"""
import re
from typing import Optional


# Known Mexican states (case-insensitive comparison)
_MX_STATES: frozenset = frozenset({
    "jalisco", "nuevo león", "nuevo leon", "ciudad de méxico", "ciudad de mexico",
    "cdmx", "estado de méxico", "estado de mexico", "puebla", "guanajuato",
    "querétaro", "queretaro", "coahuila", "sonora", "chihuahua", "tamaulipas",
    "sinaloa", "veracruz", "oaxaca", "yucatán", "yucatan", "baja california",
    "aguascalientes", "michoacán", "michoacan",
})

# Known Mexican cities (case-insensitive comparison)
_MX_CITIES: frozenset = frozenset({
    "guadalajara", "monterrey", "mexico city", "ciudad de méxico", "ciudad de mexico",
    "cdmx", "puebla", "león", "leon", "tijuana", "zapopan", "mérida", "merida",
    "querétaro", "queretaro", "san luis potosí", "san luis potosi", "mexicali",
    "chihuahua", "toluca", "culiacán", "culiacan", "aguascalientes", "morelia",
    "saltillo", "cancún", "cancun",
})

# Spanish business suffix patterns (matched case-insensitively against company name)
_MX_SUFFIX_PATTERNS: tuple = (
    r's\.a\.\s*de\s*c\.v\.',
    r'sa\s+de\s+cv\b',
    r's\.a\.p\.i\.',
    r'\bsapi\b',
    r's\.\s*de\s*r\.l\.',
    r's\s+de\s+rl\b',
    r's\.r\.l\.',
    r'\bsrl\b',
    r'\bs\.c\.\b',
    r'sociedad\s+an[oó]nima',
    r'sociedad\s+de\s+responsabilidad',
)

# Pre-compiled regex for Spanish business suffixes
_MX_SUFFIX_RE = re.compile(
    '|'.join(_MX_SUFFIX_PATTERNS),
    re.IGNORECASE,
)


def _tld_is_mx(url: Optional[str]) -> bool:
    """Return True if the URL/domain's effective TLD is .mx."""
    if not url:
        return False
    # Strip protocol and paths to get the host portion
    host = url.lower().strip()
    # Remove protocol
    for prefix in ("https://", "http://", "ftp://"):
        if host.startswith(prefix):
            host = host[len(prefix):]
            break
    # Remove path/query/fragment
    host = host.split("/")[0].split("?")[0].split("#")[0].split(":")[0]
    # Strip leading www.
    if host.startswith("www."):
        host = host[4:]
    # Check TLD
    return host.endswith(".mx")


def detect_workspace(contact: dict) -> str:
    """
    Returns 'MX' or 'US' based on contact data signals.

    Priority order (first match wins):
    1. company_country == "Mexico" (case-insensitive) → MX
    2. domain TLD ends in .mx → MX
    3. website TLD ends in .mx → MX
    4. Spanish business suffix in company name → MX
    5. company_city or company_state in known Mexican cities/states list → MX
    6. Default → US
    """
    # 1. company_country
    company_country = (contact.get("company_country") or "").strip()
    if company_country.lower() == "mexico":
        return "MX"

    # 2. domain TLD
    if _tld_is_mx(contact.get("domain")):
        return "MX"

    # 3. website TLD
    if _tld_is_mx(contact.get("website")):
        return "MX"

    # 4. Spanish business suffix in company name
    company = contact.get("company") or ""
    if company and _MX_SUFFIX_RE.search(company):
        return "MX"

    # 5. company_city or company_state in known MX locations
    company_city = (contact.get("company_city") or "").strip().lower()
    if company_city and company_city in _MX_CITIES:
        return "MX"

    company_state = (contact.get("company_state") or "").strip().lower()
    if company_state and company_state in _MX_STATES:
        return "MX"

    # 6. Default
    return "US"
