---
paths: frontend/src/**/*.{js,jsx}
---

# React Frontend Rules

## Code Style
- Use 2-space indentation
- Use functional components with hooks
- Prefer named exports over default exports
- Keep components focused and small

## State Management
- Use React Context API for global state (auth, settings)
- Use useState for local component state
- Use useEffect for side effects and API calls

## API Calls
- Use the custom fetch wrapper with Bearer token auth
- Handle loading and error states
- Show appropriate user feedback (toast notifications)

## Styling
- Use the existing CSS classes from app.css
- Follow the established color scheme (Blue, Coral, Teal, Purple)
- Maintain responsive design patterns
