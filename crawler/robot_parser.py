class CustomRobotsParser:
    def __init__(self, user_agent='*'):
        self.user_agent = user_agent
        self.allowed = []
        self.disallowed = []
        self.sitemaps = []
        self.current_user_agent = None

    def parse(self, content):
        lines = content.split('\n')
        for line in lines:
            # Remove any comment portion of the line
            line = line.split('#', 1)[0].strip()
            if not line:
                continue  # skip blank lines

            line_parts = line.split(':', 1)
            if len(line_parts) != 2:
                # Malformed line
                continue

            directive, value = line_parts
            directive = directive.strip().lower()
            value = value.strip()

            if directive == 'user-agent':
                self.current_user_agent = value
            elif directive == 'sitemap':
                self.sitemaps.append(value)
            elif self.current_user_agent == self.user_agent or self.current_user_agent == '*':
                if directive == 'allow':
                    if value is not None and value != '':
                        self.allowed.append(value)
                elif directive == 'disallow':  
                    if value is not None and value != '':
                        self.disallowed.append(value)
            

    def can_fetch(self, path):
        for disallowed_path in self.disallowed:
            if path.startswith(disallowed_path):
                for allowed_path in self.allowed:
                    if path.startswith(allowed_path):
                        return True
                return False
        return True

    def get_sitemaps(self):
        return self.sitemaps
