import React from 'react';
import { 
  Box, 
  Paper, 
  Container, 
  Typography,
  useTheme,
  useMediaQuery 
} from '@mui/material';
import { VideoCall } from '@mui/icons-material';

const AuthLayout = ({ children, title, subtitle }) => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  return (
    <Box
      sx={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        position: 'relative',
        '&::before': {
          content: '""',
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="0.05"%3E%3Ccircle cx="7" cy="7" r="7"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
          opacity: 0.3
        }
      }}
    >
      <Container maxWidth="sm" sx={{ position: 'relative', zIndex: 1 }}>
        <Paper
    
        >
          <Box sx={{ textAlign: 'center', mb: 4 }}>
            <Box 
              sx={{ 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'center',
                mb: 2
              }}
            >
              <VideoCall 
                sx={{ 
                  fontSize: 48, 
                  color: 'primary.main',
                  mr: 1
                }} 
              />
              <Typography 
                variant="h4" 
                component="h1" 
                fontWeight="bold" 
                sx={{
                  background: 'linear-gradient(45deg, #667eea 30%, #764ba2 90%)',
                  backgroundClip: 'text',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent'
                }}
              >
                MeetPro
              </Typography>
            </Box>
            
            <Typography 
              variant="h5" 
              component="h2" 
              fontWeight="600" 
              color="text.primary"
              sx={{ mb: 1 }}
            >
              {title}
            </Typography>
            
            {subtitle && (
              <Typography 
                variant="body1" 
                color="text.secondary" 
                sx={{ 
                  fontSize: '1.1rem',
                  lineHeight: 1.5
                }}
              >
                {subtitle}
              </Typography>
            )}
          </Box>
          
          {children}
        </Paper>
      </Container>
    </Box>
  );
};

export default AuthLayout;