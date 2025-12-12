// context/ThemeContext.jsx
import React, { createContext, useContext } from 'react';
import { ThemeProvider as MUIThemeProvider } from '@mui/material/styles';
import { CssBaseline } from '@mui/material';
import { useMUITheme } from '../hooks/useMUITheme';

const ThemeContext = createContext();

export const useTheme = () => {
  const context = useContext(ThemeContext);
  if (!context) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
};

export const ThemeProvider = ({ children }) => {
  const themeConfig = useMUITheme();

  return (
    <ThemeContext.Provider value={themeConfig}>
      <MUIThemeProvider theme={themeConfig.theme}>
        <CssBaseline />
        {children}
      </MUIThemeProvider>
    </ThemeContext.Provider>
  );
};