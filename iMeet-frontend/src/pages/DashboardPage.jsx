// src/pages/DashboardPage.jsx - FIXED
import React, { useEffect } from 'react';
import { Box, Container, useTheme, alpha } from '@mui/material';
import DashboardLayout from '../layouts/DashboardLayout';
import Dashboard from '../components/dashboard/Dashboard';
import { useNotifications } from '../hooks/useNotifications';

const DashboardPage = () => {
  const theme = useTheme();
  // FIXED: Use fetchNotifications (which fetches ALL types)
  const { fetchNotifications } = useNotifications();

  useEffect(() => {
    console.log('📊 Dashboard: Fetching ALL notifications (calendar + scheduled + recording)');
    
    // Fetch all notifications
    fetchNotifications();
    
    // Set up polling for real-time updates
    const pollInterval = setInterval(() => {
      console.log('🔄 Dashboard: Polling for notification updates...');
      fetchNotifications();
    }, 30000); // Poll every 30 seconds
    
    return () => {
      clearInterval(pollInterval);
    };
  }, [fetchNotifications]);

  return (
    <DashboardLayout>
      <Box
        sx={{
          flexGrow: 1,
          background: `linear-gradient(135deg, ${alpha(theme.palette.background.default, 0.9)} 0%, ${alpha(theme.palette.primary.main, 0.02)} 100%)`,
          minHeight: '100vh',
          pt: 2
        }}
      >
        <Container maxWidth="xl" sx={{ py: 3 }}>
          <Dashboard />
        </Container>
      </Box>
    </DashboardLayout>
  );
};

export default DashboardPage;