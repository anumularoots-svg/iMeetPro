// src/hooks/useMeetingTabs.js - Complete Tab Management Hook
import { useState, useCallback } from 'react';

export const useMeetingTabs = ({ showNotificationMessage }) => {
  // Tab states
  const [activeTab, setActiveTab] = useState('meeting');
  const [availableTabs, setAvailableTabs] = useState(['meeting']);

  // Get tab icon
  const getTabIcon = useCallback((tab) => {
    const icons = {
      meeting: 'VideoCall',
      whiteboard: 'WhiteboardIcon',
      recordings: 'VideoLibrary',
      settings: 'Settings',
    };
    return icons[tab] || null;
  }, []);

  // Get tab title
  const getTabTitle = useCallback((tab) => {
    return tab.charAt(0).toUpperCase() + tab.slice(1);
  }, []);

  // Add new tab
  const addTab = useCallback((tabName) => {
    setAvailableTabs(prev => {
      if (prev.includes(tabName)) {
        return prev;
      }
      return [...prev, tabName];
    });
    setActiveTab(tabName);
  }, []);

  // Close tab
  const handleCloseTab = useCallback((tabToClose) => {
    if (tabToClose === 'meeting') {
      // Cannot close meeting tab
      return;
    }

    // Remove tab from available tabs
    setAvailableTabs(prev => prev.filter(tab => tab !== tabToClose));

    // If closing active tab, switch to meeting
    if (activeTab === tabToClose) {
      setActiveTab('meeting');
    }

    showNotificationMessage(`${tabToClose} tab closed`, "info");
  }, [activeTab, showNotificationMessage]);

  // Switch to tab
  const switchToTab = useCallback((tabName) => {
    if (!availableTabs.includes(tabName)) {
      // Add tab if it doesn't exist
      setAvailableTabs(prev => [...prev, tabName]);
    }
    setActiveTab(tabName);
  }, [availableTabs]);

  // Check if tab is open
  const isTabOpen = useCallback((tabName) => {
    return availableTabs.includes(tabName);
  }, [availableTabs]);

  return {
    // State
    activeTab,
    availableTabs,
    
    // Setters
    setActiveTab,
    setAvailableTabs,
    
    // Handlers
    getTabIcon,
    getTabTitle,
    addTab,
    handleCloseTab,
    switchToTab,
    isTabOpen,
  };
};