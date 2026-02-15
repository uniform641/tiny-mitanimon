import re

def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except(ValueError, TypeError):
        return default

def chunked(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]

def remove_brackets_and_content(text):
    # Pattern to match parentheses and their content
    text = re.sub(r'\([^)]*\)', '', text)
    # Pattern to match square brackets and their content
    text = re.sub(r'\[[^\]]*\]', '', text)
    # Pattern to match curly braces and their content
    text = re.sub(r'\{[^}]*\}', '', text)
    return text.strip()