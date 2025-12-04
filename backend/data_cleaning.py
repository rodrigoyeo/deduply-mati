"""
Data Cleaning Module for Deduply
Provides functions to clean and standardize contact data
"""

import re
from typing import Optional, Dict, List, Tuple

# Common name prefixes that should stay lowercase
NAME_PARTICLES = {'van', 'von', 'de', 'del', 'della', 'di', 'da', 'le', 'la', 'du', 'des', 'el', 'al'}

# Common company suffixes to remove or standardize
COMPANY_SUFFIXES = [
    r'\s*,?\s*Inc\.?$',
    r'\s*,?\s*LLC\.?$',
    r'\s*,?\s*L\.?L\.?C\.?$',
    r'\s*,?\s*Corp\.?$',
    r'\s*,?\s*Corporation$',
    r'\s*,?\s*Co\.?$',
    r'\s*,?\s*Company$',
    r'\s*,?\s*Ltd\.?$',
    r'\s*,?\s*Limited$',
    r'\s*,?\s*LP\.?$',
    r'\s*,?\s*L\.?P\.?$',
    r'\s*,?\s*LLP\.?$',
    r'\s*,?\s*P\.?C\.?$',
    r'\s*,?\s*PLLC\.?$',
    r'\s*,?\s*NA\.?$',
    r'\s*,?\s*N\.?A\.?$',
]

# Special name cases (McDonald, O'Brien, etc.)
SPECIAL_NAME_PATTERNS = {
    r"^mc": lambda m: "Mc",
    r"^mac(?=[aeiou])": lambda m: "Mac",
    r"^o'": lambda m: "O'",
    r"^d'": lambda m: "D'",
}


def clean_name(name: Optional[str], preserve_case_if_mixed: bool = True) -> Optional[str]:
    """
    Clean and properly capitalize a name.

    - Converts ALL CAPS to Title Case
    - Handles special cases: McDonald, O'Brien, van der Berg
    - Preserves already mixed-case names if preserve_case_if_mixed is True

    Args:
        name: The name to clean
        preserve_case_if_mixed: If True, don't change names that already have mixed case

    Returns:
        Cleaned name or None if input was None/empty
    """
    if not name or not name.strip():
        return None

    name = name.strip()

    # Check if name is already mixed case (has both upper and lower)
    if preserve_case_if_mixed:
        has_upper = any(c.isupper() for c in name)
        has_lower = any(c.islower() for c in name)
        if has_upper and has_lower:
            return name  # Already properly cased

    # If all caps or all lower, convert to title case
    if name.isupper() or name.islower():
        name = name.title()

    # Handle special patterns
    words = name.split()
    cleaned_words = []

    for i, word in enumerate(words):
        # Skip particles in middle of name (van, von, de, etc.)
        if i > 0 and word.lower() in NAME_PARTICLES:
            cleaned_words.append(word.lower())
            continue

        # Handle Mc/Mac/O'/D' prefixes
        word_lower = word.lower()
        for pattern, replacement in SPECIAL_NAME_PATTERNS.items():
            if re.match(pattern, word_lower):
                # Capitalize the letter after the prefix
                match = re.match(pattern, word_lower)
                prefix_len = match.end()
                if len(word) > prefix_len:
                    word = replacement(match) + word[prefix_len].upper() + word[prefix_len + 1:].lower()
                break

        cleaned_words.append(word)

    return ' '.join(cleaned_words)


def clean_company_name(company: Optional[str], domain: Optional[str] = None,
                       remove_suffixes: bool = True,
                       use_domain_hint: bool = True) -> Tuple[Optional[str], Optional[str]]:
    """
    Clean and standardize a company name.

    - Removes common suffixes (Inc., LLC, etc.)
    - Extracts clean name from parentheses patterns
    - Uses domain as hint for shorter name

    Args:
        company: The company name to clean
        domain: Optional domain to help identify the core company name
        remove_suffixes: Whether to remove Inc., LLC, etc.
        use_domain_hint: Whether to use domain to find shorter name

    Returns:
        Tuple of (cleaned_name, suggestion_reason)
    """
    if not company or not company.strip():
        return None, None

    original = company.strip()
    cleaned = original
    reason = None

    # Check for parenthetical patterns like "Company Name (ACRONYM)" or "(formerly XYZ)"
    paren_match = re.search(r'\(([^)]+)\)', cleaned)
    if paren_match:
        paren_content = paren_match.group(1).strip()
        before_paren = cleaned[:paren_match.start()].strip()

        # Handle "formerly" pattern first
        if 'formerly' in paren_content.lower():
            cleaned = before_paren
            reason = "Removed 'formerly' reference"
        # Check if content in parentheses matches domain
        elif domain and use_domain_hint:
            domain_name = extract_domain_name(domain)

            if domain_name:
                paren_clean = paren_content.lower().replace(' ', '').replace('&', '')
                domain_clean = domain_name.lower()

                # If parenthetical content matches domain exactly
                if paren_clean == domain_clean:
                    cleaned = paren_content
                    reason = f"Matched domain '{domain}'"
                # If parenthetical content is at start of domain (e.g., EDSS matches edssenergy)
                elif domain_clean.startswith(paren_clean) and len(paren_clean) >= 2:
                    cleaned = paren_content
                    reason = f"Matched domain prefix '{domain}'"
                # If domain starts with parenthetical content
                elif paren_clean.startswith(domain_clean) and len(domain_clean) >= 3:
                    cleaned = paren_content
                    reason = f"Matched domain '{domain}'"
                # Otherwise just remove the parenthetical part
                elif before_paren:
                    cleaned = before_paren
                    reason = "Removed parenthetical abbreviation"

    # Remove common suffixes if requested
    if remove_suffixes and cleaned == original:  # Only if we haven't already modified
        for pattern in COMPANY_SUFFIXES:
            new_cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
            if new_cleaned != cleaned:
                cleaned = new_cleaned.strip()
                if not reason:
                    reason = "Removed business suffix"

    # Clean up any remaining artifacts
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = re.sub(r'^[\[\(]|[\]\)]$', '', cleaned).strip()

    if cleaned == original:
        return original, None

    return cleaned, reason


def extract_domain_name(domain: Optional[str]) -> Optional[str]:
    """
    Extract the company name portion from a domain.

    Example: "acmewidgets.com" -> "acmewidgets"
    """
    if not domain:
        return None

    # Remove protocol if present
    domain = re.sub(r'^https?://', '', domain)
    # Remove www.
    domain = re.sub(r'^www\.', '', domain)
    # Get just the domain name (before .com, .net, etc.)
    parts = domain.split('.')
    if parts:
        return parts[0]
    return None


def suggest_company_from_domain(domain: Optional[str]) -> Optional[str]:
    """
    Suggest a clean company name based on domain.

    Example: "acme-widgets.com" -> "Acme Widgets"
    """
    name = extract_domain_name(domain)
    if not name:
        return None

    # Replace hyphens/underscores with spaces
    name = re.sub(r'[-_]', ' ', name)

    # Title case
    name = name.title()

    return name


def preview_name_cleaning(contacts: List[Dict]) -> List[Dict]:
    """
    Preview name cleaning changes without applying them.

    Returns list of contacts that would be changed with before/after values.
    """
    changes = []

    for contact in contacts:
        first_name = contact.get('first_name')
        last_name = contact.get('last_name')

        cleaned_first = clean_name(first_name, preserve_case_if_mixed=False)
        cleaned_last = clean_name(last_name, preserve_case_if_mixed=False)

        first_changed = cleaned_first and cleaned_first != first_name
        last_changed = cleaned_last and cleaned_last != last_name

        if first_changed or last_changed:
            changes.append({
                'id': contact.get('id'),
                'first_name': {
                    'before': first_name,
                    'after': cleaned_first,
                    'changed': first_changed
                },
                'last_name': {
                    'before': last_name,
                    'after': cleaned_last,
                    'changed': last_changed
                }
            })

    return changes


def preview_company_cleaning(contacts: List[Dict]) -> List[Dict]:
    """
    Preview company name cleaning changes without applying them.

    Returns list of contacts that would be changed with before/after values.
    """
    changes = []

    for contact in contacts:
        company = contact.get('company')
        domain = contact.get('domain')

        cleaned, reason = clean_company_name(company, domain)

        if reason:  # Only if there's a suggested change
            changes.append({
                'id': contact.get('id'),
                'company': {
                    'before': company,
                    'after': cleaned,
                    'reason': reason
                },
                'domain': domain
            })

    return changes


# Analysis functions for reporting
def analyze_data_quality(contacts: List[Dict]) -> Dict:
    """
    Analyze contacts for data quality issues.

    Returns statistics about what needs cleaning.
    """
    stats = {
        'total_contacts': len(contacts),
        'names': {
            'all_caps_first': 0,
            'all_caps_last': 0,
            'single_letter_first': 0,
            'single_letter_last': 0,
            'needs_cleaning': 0
        },
        'companies': {
            'has_parentheses': 0,
            'has_suffix': 0,
            'domain_mismatch': 0,
            'needs_cleaning': 0
        }
    }

    names_needing_cleaning = 0
    companies_needing_cleaning = 0

    for contact in contacts:
        first = contact.get('first_name', '') or ''
        last = contact.get('last_name', '') or ''
        company = contact.get('company', '') or ''
        domain = contact.get('domain', '') or ''

        # Name analysis
        if first.isupper() and len(first) > 1:
            stats['names']['all_caps_first'] += 1
        if last.isupper() and len(last) > 1:
            stats['names']['all_caps_last'] += 1
        if len(first) == 1:
            stats['names']['single_letter_first'] += 1
        if len(last) == 1:
            stats['names']['single_letter_last'] += 1

        # Check if name would actually be cleaned
        first_cleaned = clean_name(first, preserve_case_if_mixed=False) if first else first
        last_cleaned = clean_name(last, preserve_case_if_mixed=False) if last else last
        if first_cleaned != first or last_cleaned != last:
            names_needing_cleaning += 1

        # Company analysis
        if '(' in company:
            stats['companies']['has_parentheses'] += 1
        if re.search(r'\b(Inc|LLC|Corp|Ltd|Co)\b', company, re.IGNORECASE):
            stats['companies']['has_suffix'] += 1

        # Check if company would actually be cleaned
        if company:
            cleaned_company, reason = clean_company_name(company, domain)
            if reason:
                companies_needing_cleaning += 1

        # Check domain mismatch
        if company and domain:
            domain_name = extract_domain_name(domain)
            if domain_name and domain_name.lower() not in company.lower().replace(' ', ''):
                stats['companies']['domain_mismatch'] += 1

    # Use actual counts of what would be cleaned
    stats['names']['needs_cleaning'] = names_needing_cleaning
    stats['companies']['needs_cleaning'] = companies_needing_cleaning

    return stats
