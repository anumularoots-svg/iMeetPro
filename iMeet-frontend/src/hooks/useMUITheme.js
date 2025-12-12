// hooks/useMUITheme.js
import { useState, useCallback, useEffect } from 'react';
import { createTheme } from '@mui/material/styles';
import { useLocalStorage } from './useLocalStorage';

export const useMUITheme = () => {
  const [themeMode, setThemeMode] = useLocalStorage('themeMode', 'light');
  const [primaryColor, setPrimaryColor] = useLocalStorage('primaryColor', '#1976d2');
  const [customSettings, setCustomSettings] = useLocalStorage('customSettings', {});

  // Create dynamic theme
  const theme = createTheme({
    palette: {
      mode: themeMode,
      primary: {
        main: primaryColor,
      },
      secondary: {
        main: themeMode === 'dark' ? '#f50057' : '#dc004e',
      },
      background: {
        default: themeMode === 'dark' ? '#121212' : '#f5f5f5',
        paper: themeMode === 'dark' ? '#1e1e1e' : '#ffffff',
      },
    },
    typography: {
      fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
      h1: {
        fontSize: '2.5rem',
        fontWeight: 600,
      },
      h2: {
        fontSize: '2rem',
        fontWeight: 500,
      },
      h3: {
        fontSize: '1.75rem',
        fontWeight: 500,
      },
      h4: {
        fontSize: '1.5rem',
        fontWeight: 500,
      },
      h5: {
        fontSize: '1.25rem',
        fontWeight: 500,
      },
      h6: {
        fontSize: '1rem',
        fontWeight: 500,
      },
    },
    shape: {
      borderRadius: 12,
    },
    components: {
      MuiButton: {
        styleOverrides: {
          root: {
            textTransform: 'none',
            borderRadius: 8,
            fontWeight: 500,
          },
        },
      },
      MuiCard: {
        styleOverrides: {
          root: {
            borderRadius: 16,
            boxShadow: themeMode === 'dark' 
              ? '0 4px 20px rgba(0,0,0,0.3)' 
              : '0 4px 20px rgba(0,0,0,0.1)',
          },
        },
      },
      MuiAppBar: {
        styleOverrides: {
          root: {
            backgroundColor: themeMode === 'dark' ? '#1e1e1e' : primaryColor,
          },
        },
      },
    },
    ...customSettings,
  });

  // Toggle theme mode
  const toggleThemeMode = useCallback(() => {
    setThemeMode(prev => prev === 'light' ? 'dark' : 'light');
  }, [setThemeMode]);

  // Update primary color
  const updatePrimaryColor = useCallback((color) => {
    setPrimaryColor(color);
  }, [setPrimaryColor]);

  // Update custom settings
  const updateCustomSettings = useCallback((settings) => {
    setCustomSettings(prev => ({ ...prev, ...settings }));
  }, [setCustomSettings]);

  // Reset to defaults
  const resetTheme = useCallback(() => {
    setThemeMode('light');
    setPrimaryColor('#1976d2');
    setCustomSettings({});
  }, [setThemeMode, setPrimaryColor, setCustomSettings]);

  // Predefined color schemes
  const colorSchemes = [
    { name: 'Blue', color: '#1976d2' },
    { name: 'Purple', color: '#7b1fa2' },
    { name: 'Green', color: '#388e3c' },
    { name: 'Orange', color: '#f57c00' },
    { name: 'Red', color: '#d32f2f' },
    { name: 'Teal', color: '#00796b' },
  ];

  return {
    theme,
    themeMode,
    primaryColor,
    customSettings,
    toggleThemeMode,
    updatePrimaryColor,
    updateCustomSettings,
    resetTheme,
    colorSchemes,
  };
};