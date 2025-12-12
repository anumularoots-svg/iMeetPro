import { createTheme } from '@mui/material/styles';
import { lightPalette, darkPalette, meetingColors } from './palette';
import { typography } from './typography';
import { shadows } from './shadows';
import { breakpoints } from './breakpoints';

// Create theme function
const createAppTheme = (mode = 'light') => {
  const palette = mode === 'dark' ? darkPalette : lightPalette;
  
  return createTheme({
    palette,
    typography,
    shadows: shadows[mode],
    breakpoints,
    
    // Custom spacing
    spacing: 8,
    
    // Shape configuration
    shape: {
      borderRadius: 12
    },
    
    // Transitions
    transitions: {
      duration: {
        shortest: 150,
        shorter: 200,
        short: 250,
        standard: 300,
        complex: 375,
        enteringScreen: 225,
        leavingScreen: 195
      },
      easing: {
        easeInOut: 'cubic-bezier(0.4, 0, 0.2, 1)',
        easeOut: 'cubic-bezier(0.0, 0, 0.2, 1)',
        easeIn: 'cubic-bezier(0.4, 0, 1, 1)',
        sharp: 'cubic-bezier(0.4, 0, 0.6, 1)'
      }
    },
    
    // Z-index configuration
    zIndex: {
      mobileStepper: 1000,
      fab: 1050,
      speedDial: 1050,
      appBar: 1100,
      drawer: 1200,
      modal: 1300,
      snackbar: 1400,
      tooltip: 1500,
      meetingControls: 1600,
      videoOverlay: 1700
    },
    
    // Component overrides
    components: {
      // Button customization
      MuiButton: {
        styleOverrides: {
          root: {
            borderRadius: 8,
            textTransform: 'none',
            fontWeight: 600,
            boxShadow: 'none',
            '&:hover': {
              boxShadow: '0 2px 8px rgba(0,0,0,0.15)'
            }
          },
          contained: {
            background: 'linear-gradient(45deg, #1976d2, #42a5f5)',
            '&:hover': {
              background: 'linear-gradient(45deg, #1565c0, #1976d2)'
            }
          }
        }
      },
      
      // Card customization
      MuiCard: {
        styleOverrides: {
          root: {
            borderRadius: 16,
            boxShadow: mode === 'dark' 
              ? '0 4px 20px rgba(0,0,0,0.5)' 
              : '0 4px 20px rgba(0,0,0,0.1)',
            transition: 'transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out',
            '&:hover': {
              transform: 'translateY(-2px)',
              boxShadow: mode === 'dark'
                ? '0 8px 30px rgba(0,0,0,0.7)'
                : '0 8px 30px rgba(0,0,0,0.15)'
            }
          }
        }
      },
      
      // Paper customization
      MuiPaper: {
        styleOverrides: {
          root: {
            borderRadius: 12
          },
          elevation1: {
            boxShadow: mode === 'dark'
              ? '0 2px 8px rgba(0,0,0,0.4)'
              : '0 2px 8px rgba(0,0,0,0.08)'
          }
        }
      },
      
      // TextField customization
      MuiTextField: {
        styleOverrides: {
          root: {
            '& .MuiOutlinedInput-root': {
              borderRadius: 10,
              '&:hover .MuiOutlinedInput-notchedOutline': {
                borderColor: palette.primary.main
              }
            }
          }
        }
      },
      
      // Dialog customization
      MuiDialog: {
        styleOverrides: {
          paper: {
            borderRadius: 16,
            maxWidth: '90vw'
          }
        }
      },
      
      // AppBar customization
      MuiAppBar: {
        styleOverrides: {
          root: {
            background: mode === 'dark' 
              ? 'linear-gradient(90deg, #1e1e1e, #2a2a2a)'
              : 'linear-gradient(90deg, #ffffff, #f8f9fa)',
            backdropFilter: 'blur(10px)',
            borderBottom: `1px solid ${palette.divider}`
          }
        }
      },
      
      // Chip customization
      MuiChip: {
        styleOverrides: {
          root: {
            borderRadius: 20,
            fontWeight: 500
          }
        }
      },
      
      // Tab customization
      MuiTab: {
        styleOverrides: {
          root: {
            textTransform: 'none',
            fontWeight: 600,
            fontSize: '0.875rem'
          }
        }
      }
    },
    
    // Custom theme properties
    custom: {
      meeting: meetingColors,
      gradients: {
        primary: 'linear-gradient(45deg, #1976d2, #42a5f5)',
        secondary: 'linear-gradient(45deg, #dc004e, #ff5983)',
        success: 'linear-gradient(45deg, #2e7d32, #4caf50)',
        background: mode === 'dark'
          ? 'linear-gradient(135deg, #121212, #1e1e1e)'
          : 'linear-gradient(135deg, #fafafa, #ffffff)'
      },
      animations: {
        slideIn: 'slideIn 0.3s ease-out',
        fadeIn: 'fadeIn 0.2s ease-in',
        pulse: 'pulse 2s infinite',
        bounce: 'bounce 0.5s ease-out'
      }
    }
  });
};

// Export themes
export const lightTheme = createAppTheme('light');
export const darkTheme = createAppTheme('dark');

// Default export
export default createAppTheme;