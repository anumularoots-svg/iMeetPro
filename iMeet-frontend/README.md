# Video Meeting App Frontend

A professional Google Meet-like video meeting application built with React, Material-UI, and WebRTC.

## 🚀 Features

### 📱 Main Features
- **Multiple Meeting Types**: Instant, Scheduled, and Calendar-integrated meetings
- **HD Video/Audio**: Professional quality video conferencing with adaptive streaming
- **Real-time Chat**: Text messaging, file sharing, and emoji integration
- **Screen Sharing**: Full screen, window, or tab sharing capabilities
- **Interactive Reactions**: Emoji reactions with animations
- **AI-Powered Analytics**: Attendance tracking and engagement metrics
- **Recording & Transcription**: Local and cloud recording with automatic transcription
- **Waiting Room**: Pre-meeting lobby with device testing
- **Calendar Integration**: Google Calendar, Outlook, and Apple Calendar sync

### 🎛️ Meeting Controls
- **Host Controls**: Participant management, mute/unmute, remove participants, recording control
- **Participant Controls**: Self mute/unmute, camera on/off, chat, reactions, raise hand
- **Advanced Permissions**: Role-based access control and dynamic permission management

### 🔧 Technical Features
- **Responsive Design**: Mobile-first approach with cross-device compatibility
- **Real-time Communication**: WebRTC for peer-to-peer communication
- **WebSocket Integration**: Real-time updates and notifications
- **Progressive Web App**: Offline support and app-like experience
- **Accessibility**: WCAG compliant with screen reader support

## 🛠️ Technology Stack

- **Frontend Framework**: React 18 + Vite
- **UI Library**: Material-UI (MUI) v5
- **Routing**: React Router v6
- **State Management**: React Context API
- **Real-time**: Socket.io Client + WebRTC
- **Styling**: Emotion + CSS-in-JS
- **Testing**: Jest + React Testing Library
- **Build Tool**: Vite
- **Package Manager**: npm/yarn

## 📋 Prerequisites

- Node.js (v16.0.0 or higher)
- npm (v8.0.0 or higher) or yarn
- Modern web browser with WebRTC support

## 🚀 Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/meeting-app-frontend.git
cd meeting-app-frontend
```

### 2. Install Dependencies
```bash
npm install
# or
yarn install
```

### 3. Environment Setup
```bash
# Copy environment template
cp .env.example .env

# Edit .env file with your configuration
nano .env
```

### 4. Start Development Server
```bash
npm run dev
# or
yarn dev
```

The application will be available at `http://localhost:5173`

## 🏗️ Project Structure

```
src/
├── components/          # Reusable UI components
│   ├── auth/           # Authentication components
│   ├── dashboard/      # Dashboard components
│   ├── meeting/        # Meeting creation & management
│   ├── scheduling/     # Meeting scheduling components
│   ├── calendar/       # Calendar integration
│   ├── invitations/    # Invitation management
│   ├── video/          # Video streaming components
│   ├── controls/       # Meeting controls
│   ├── reactions/      # Reactions & emoji system
│   ├── chat/           # Chat system
│   ├── participants/   # Participant management
│   ├── interactions/   # Interactive features
│   ├── recording/      # Recording & playback
│   ├── analytics/      # AI analytics & metrics
│   └── common/         # Shared components
├── pages/              # Page-level components
├── services/           # API & external services
├── hooks/              # Custom React hooks
├── context/            # React Context providers
├── utils/              # Utility functions
├── theme/              # Material-UI theme
├── layouts/            # Layout components
└── assets/             # Static assets
```

## 🔧 Available Scripts

```bash
# Development
npm run dev              # Start development server
npm run build           # Build for production
npm run preview         # Preview production build

# Testing
npm run test            # Run tests
npm run test:watch      # Run tests in watch mode
npm run test:coverage   # Run tests with coverage

# Code Quality
npm run lint            # Run ESLint
npm run lint:fix        # Fix ESLint issues
```

## 🌐 Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# API Configuration
VITE_API_BASE_URL=https://192.168.48.201:8111/api
VITE_WS_BASE_URL=ws://192.168.48.201:8111/ws/meeting/(?P<meeting_id>[^/]+)/$

# WebRTC Configuration
VITE_STUN_SERVERS=stun:stun.l.google.com:19302
VITE_TURN_SERVERS=turn:your-turn-server.com:3478

# Calendar Integration
VITE_GOOGLE_CALENDAR_CLIENT_ID=your-google-client-id
VITE_MICROSOFT_CALENDAR_CLIENT_ID=your-microsoft-client-id

# App Configuration
VITE_APP_NAME=Meeting App
VITE_MAX_PARTICIPANTS=50
VITE_MAX_MEETING_DURATION=180
```

## 📱 Component Architecture

### Core Components

#### Authentication Flow
- `Login.jsx` - User login form
- `Register.jsx` - User registration
- `ForgotPassword.jsx` - Password reset
- `EmailVerification.jsx` - Email verification

#### Meeting Management
- `CreateMeeting.jsx` - Meeting creation interface
- `JoinMeeting.jsx` - Meeting join interface
- `WaitingRoom.jsx` - Pre-meeting lobby
- `MeetingRoom.jsx` - Main meeting interface

#### Video System
- `VideoPlayer.jsx` - Individual video stream
- `VideoGrid.jsx` - Multiple participant layout
- `ScreenShare.jsx` - Screen sharing functionality
- `VideoQuality.jsx` - Quality controls

#### Communication
- `ChatPanel.jsx` - Chat interface
- `MessageList.jsx` - Message display
- `ReactionsPanel.jsx` - Emoji reactions
- `ParticipantsList.jsx` - Participant management

## 🔌 API Integration

### Authentication Endpoints
```javascript
POST /api/auth/login
POST /api/auth/register
POST /api/auth/verify-email
POST /api/auth/forgot-password
POST /api/auth/reset-password
```

### Meeting Endpoints
```javascript
POST /api/meetings/create
GET  /api/meetings/:id
POST /api/meetings/:id/join
PUT  /api/meetings/:id/update
DELETE /api/meetings/:id
```

### Real-time Events
```javascript
// WebSocket Events
user-joined
user-left
message-sent
screen-share-started
participant-muted
recording-started
hand-raised
reaction-sent
```

## 🎨 Theming & Customization

The app uses Material-UI's theming system:

```javascript
// theme/theme.js
export const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
  },
  typography: {
    fontFamily: 'Roboto, Arial, sans-serif',
  },
});
```

## 📱 Responsive Design

- **Mobile First**: Optimized for mobile devices
- **Breakpoints**: 
  - xs: 0px
  - sm: 600px
  - md: 900px
  - lg: 1200px
  - xl: 1536px

## 🧪 Testing

```bash
# Run all tests
npm test

# Run specific test file
npm test -- Login.test.jsx

# Run tests with coverage
npm run test:coverage
```

## 🚀 Deployment

### Build for Production
```bash
npm run build
```

### Deploy to Netlify
```bash
# Install Netlify CLI
npm install -g netlify-cli

# Deploy
netlify deploy --prod --dir=dist
```

### Deploy to Vercel
```bash
# Install Vercel CLI
npm install -g vercel

# Deploy
vercel --prod
```

## 🔒 Security Features

- **JWT Authentication**: Secure token-based authentication
- **Input Validation**: Client-side form validation
- **XSS Protection**: Sanitized user inputs
- **CORS Configuration**: Cross-origin request security
- **HTTPS Enforcement**: SSL/TLS encryption

## 🎯 Performance Optimization

- **Code Splitting**: Lazy loading of components
- **Bundle Optimization**: Webpack optimization
- **Image Optimization**: Compressed assets
- **Caching Strategy**: Browser and service worker caching
- **Virtual Scrolling**: Efficient list rendering

## 📊 Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)
- Mobile browsers (iOS Safari, Chrome Mobile)

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support, email support@yourapp.com or join our Slack channel.

## 🙏 Acknowledgments

- [React](https://reactjs.org/)
- [Material-UI](https://mui.com/)
- [WebRTC](https://webrtc.org/)
- [Socket.io](https://socket.io/)
- [Vite](https://vitejs.dev/)

---

Built with ❤️ by Your Development Team