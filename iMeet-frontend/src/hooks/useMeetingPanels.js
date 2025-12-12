// src/hooks/useMeetingPanels.js - Complete Panel Management Hook
import { useState, useCallback, useEffect } from 'react';

export const useMeetingPanels = () => {
  // Panel states
  const [chatOpen, setChatOpen] = useState(false);
  const [participantsOpen, setParticipantsOpen] = useState(false);
  const [reactionsOpen, setReactionsOpen] = useState(false);
  const [handRaiseOpen, setHandRaiseOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [showToggleMenu, setShowToggleMenu] = useState(false);
  
  // Last click time for double-click detection
  const [lastParticipantsClickTime, setLastParticipantsClickTime] = useState(0);

  // Close all panels
  const closeAllPanels = useCallback(() => {
    setChatOpen(false);
    setParticipantsOpen(false);
    setReactionsOpen(false);
    setHandRaiseOpen(false);
    setSettingsOpen(false);
    setShowToggleMenu(false);
  }, []);

  // Toggle chat - closes other panels
  const handleToggleChat = useCallback(() => {
    const willOpen = !chatOpen;
    
    if (willOpen) {
      // Close ALL other panels when opening chat
      setParticipantsOpen(false);
      setReactionsOpen(false);
      setHandRaiseOpen(false);
      setSettingsOpen(false);
      setShowToggleMenu(false);
    }
    
    setChatOpen(willOpen);
  }, [chatOpen]);

  // Toggle participants with double-click detection
  const handleParticipantsButtonClick = useCallback(() => {
    const now = Date.now();
    const timeDiff = now - lastParticipantsClickTime;

    if (timeDiff < 400) {
      // Double click detected - close
      if (participantsOpen) {
        setParticipantsOpen(false);
      }
    } else {
      // Single click - toggle and close others
      const willOpen = !participantsOpen;
      
      if (willOpen) {
        setChatOpen(false);
        setReactionsOpen(false);
        setHandRaiseOpen(false);
        setSettingsOpen(false);
        setShowToggleMenu(false);
      }
      
      setParticipantsOpen(willOpen);
    }

    setLastParticipantsClickTime(now);
  }, [lastParticipantsClickTime, participantsOpen]);

  // Toggle reactions
  const handleToggleReactions = useCallback(() => {
    const willOpen = !reactionsOpen;
    
    if (willOpen) {
      setChatOpen(false);
      setParticipantsOpen(false);
      setHandRaiseOpen(false);
      setSettingsOpen(false);
      setShowToggleMenu(false);
    }
    
    setReactionsOpen(willOpen);
  }, [reactionsOpen]);

  // Toggle hand raise
  const handleToggleHandRaise = useCallback(() => {
    const willOpen = !handRaiseOpen;
    
    if (willOpen) {
      setChatOpen(false);
      setParticipantsOpen(false);
      setReactionsOpen(false);
      setSettingsOpen(false);
      setShowToggleMenu(false);
    }
    
    setHandRaiseOpen(willOpen);
  }, [handRaiseOpen]);

  // Toggle settings
  const handleToggleSettings = useCallback(() => {
    const willOpen = !settingsOpen;
    
    if (willOpen) {
      setChatOpen(false);
      setParticipantsOpen(false);
      setReactionsOpen(false);
      setHandRaiseOpen(false);
      setShowToggleMenu(false);
    }
    
    setSettingsOpen(willOpen);
  }, [settingsOpen]);

  // Toggle menu
  const handleToggleMenu = useCallback(() => {
    const willOpen = !showToggleMenu;
    
    if (willOpen) {
      setChatOpen(false);
      setParticipantsOpen(false);
      setReactionsOpen(false);
      setHandRaiseOpen(false);
      setSettingsOpen(false);
    }
    
    setShowToggleMenu(willOpen);
  }, [showToggleMenu]);

  // Open chat and close others
  const openChatPanel = useCallback(() => {
    setParticipantsOpen(false);
    setReactionsOpen(false);
    setHandRaiseOpen(false);
    setSettingsOpen(false);
    setShowToggleMenu(false);
    setChatOpen(true);
  }, []);

  // Open participants and close others
  const openParticipantsPanel = useCallback(() => {
    setChatOpen(false);
    setReactionsOpen(false);
    setHandRaiseOpen(false);
    setSettingsOpen(false);
    setShowToggleMenu(false);
    setParticipantsOpen(true);
  }, []);

  // Enforce mutual exclusivity
  useEffect(() => {
    const openPanels = [
      chatOpen && 'chat',
      participantsOpen && 'participants',
      reactionsOpen && 'reactions',
      handRaiseOpen && 'handRaise',
      showToggleMenu && 'menu',
      settingsOpen && 'settings'
    ].filter(Boolean);

    if (openPanels.length > 1) {
      console.warn('Multiple panels open simultaneously:', openPanels);
      const keepOpen = openPanels[openPanels.length - 1];
      
      if (keepOpen !== 'chat') setChatOpen(false);
      if (keepOpen !== 'participants') setParticipantsOpen(false);
      if (keepOpen !== 'reactions') setReactionsOpen(false);
      if (keepOpen !== 'handRaise') setHandRaiseOpen(false);
      if (keepOpen !== 'menu') setShowToggleMenu(false);
      if (keepOpen !== 'settings') setSettingsOpen(false);
    }
  }, [chatOpen, participantsOpen, reactionsOpen, handRaiseOpen, showToggleMenu, settingsOpen]);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyPress = (event) => {
      // Only handle if no input is focused
      if (
        document.activeElement.tagName === 'INPUT' ||
        document.activeElement.tagName === 'TEXTAREA' ||
        document.activeElement.isContentEditable
      ) {
        return;
      }

      if (event.key === 'c' && (event.ctrlKey || event.metaKey)) {
        event.preventDefault();
        handleToggleChat();
      } else if (event.key === 'p' && (event.ctrlKey || event.metaKey)) {
        event.preventDefault();
        handleParticipantsButtonClick();
      } else if (event.key === 'Escape') {
        if (chatOpen || participantsOpen || reactionsOpen || handRaiseOpen) {
          closeAllPanels();
        }
      }
    };

    document.addEventListener('keydown', handleKeyPress);
    return () => document.removeEventListener('keydown', handleKeyPress);
  }, [handleToggleChat, handleParticipantsButtonClick, chatOpen, participantsOpen, reactionsOpen, handRaiseOpen, closeAllPanels]);

  // Click outside to close panels
  useEffect(() => {
    const handleClickOutside = (event) => {
      const isControlButton = event.target.closest('button[aria-label], .MuiIconButton-root');
      if (isControlButton) return;

      const isChatPanel = event.target.closest('.chat-panel-container, [class*="ChatPanel"]');
      const isParticipantsPanel = event.target.closest(
        '.participants-panel-container, [class*="ParticipantsList"], [class*="MuiList"], [class*="MuiMenu"], [class*="MuiDialog"]'
      );
      const isReactionsPanel = event.target.closest('.reactions-panel-container, [class*="ReactionsManager"]');
      const isToggleMenu = event.target.closest('.toggle-menu-container, [class*="MuiCard"]');
      const isHandRaisePanel = event.target.closest('.hand-raise-panel-container');
      const isMuiElement = event.target.closest(
        '.MuiPopover-root, .MuiModal-root, .MuiBackdrop-root, .MuiMenu-root, .MuiMenuItem-root, .MuiListItem-root'
      );

      if (isChatPanel || isParticipantsPanel || isReactionsPanel || isToggleMenu || isHandRaisePanel || isMuiElement) {
        return;
      }

      // Close all open panels
      if (chatOpen) setChatOpen(false);
      if (participantsOpen) setParticipantsOpen(false);
      if (showToggleMenu) setShowToggleMenu(false);
      if (handRaiseOpen) setHandRaiseOpen(false);
      if (reactionsOpen) setReactionsOpen(false);
    };

    document.addEventListener('mousedown', handleClickOutside, true);
    return () => document.removeEventListener('mousedown', handleClickOutside, true);
  }, [chatOpen, participantsOpen, showToggleMenu, handRaiseOpen, reactionsOpen]);

  return {
    // States
    chatOpen,
    participantsOpen,
    reactionsOpen,
    handRaiseOpen,
    settingsOpen,
    showToggleMenu,
    lastParticipantsClickTime,
    
    // Setters
    setChatOpen,
    setParticipantsOpen,
    setReactionsOpen,
    setHandRaiseOpen,
    setSettingsOpen,
    setReactionsOpen,
    setShowToggleMenu,
    setLastParticipantsClickTime,
    
    // Handlers
    closeAllPanels,
    handleToggleChat,
    handleParticipantsButtonClick,
    handleToggleReactions,
    handleToggleHandRaise,
    handleToggleSettings,
    handleToggleMenu,
    openChatPanel,
    openParticipantsPanel,
  };
};