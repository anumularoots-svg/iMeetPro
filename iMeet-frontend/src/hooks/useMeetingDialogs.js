// src/hooks/useMeetingDialogs.js - Complete Dialog Management Hook
import { useState, useCallback } from 'react';

export const useMeetingDialogs = () => {
  // Dialog states
  const [showLeaveDialog, setShowLeaveDialog] = useState(false);
  const [showEndMeetingDialog, setShowEndMeetingDialog] = useState(false);
  const [showScreenShareRequest, setShowScreenShareRequest] = useState(false);
  const [showShareDialog, setShowShareDialog] = useState(false);
  const [showCoHostDialog, setShowCoHostDialog] = useState(false);
  const [showRecordingDialog, setShowRecordingDialog] = useState(false);
  const [showRecordingMethodDialog, setShowRecordingMethodDialog] = useState(false);
  const [showMeetingLinkPopup, setShowMeetingLinkPopup] = useState(true);
  const [meetingLinkMinimized, setMeetingLinkMinimized] = useState(false);
  
  // ✅ NEW - Screen Share Stopped Dialog
  const [showScreenShareStopped, setShowScreenShareStopped] = useState(false);
  const [screenShareStoppedBy, setScreenShareStoppedBy] = useState(null);

  // Role change dialog state
  const [selectedParticipantForRole, setSelectedParticipantForRole] = useState(null);
  const [roleChangeAction, setRoleChangeAction] = useState(null);

  // Recording dialog resolve
  const [recordingDialogResolve, setRecordingDialogResolve] = useState(null);

  // Open leave dialog
  const openLeaveDialog = useCallback(() => {
    setShowLeaveDialog(true);
  }, []);

  // Close leave dialog
  const closeLeaveDialog = useCallback(() => {
    setShowLeaveDialog(false);
  }, []);

  // Open end meeting dialog
  const openEndMeetingDialog = useCallback(() => {
    setShowEndMeetingDialog(true);
  }, []);

  // Close end meeting dialog
  const closeEndMeetingDialog = useCallback(() => {
    setShowEndMeetingDialog(false);
  }, []);

  // Open screen share request dialog
  const openScreenShareRequestDialog = useCallback(() => {
    setShowScreenShareRequest(true);
  }, []);

  // Close screen share request dialog
  const closeScreenShareRequestDialog = useCallback(() => {
    setShowScreenShareRequest(false);
  }, []);

  // Open share dialog
  const openShareDialog = useCallback(() => {
    setShowShareDialog(true);
  }, []);

  // Close share dialog
  const closeShareDialog = useCallback(() => {
    setShowShareDialog(false);
  }, []);

  // Open co-host dialog
  const openCoHostDialog = useCallback((participant, action) => {
    setSelectedParticipantForRole(participant);
    setRoleChangeAction(action);
    setShowCoHostDialog(true);
  }, []);

  // Close co-host dialog
  const closeCoHostDialog = useCallback(() => {
    setShowCoHostDialog(false);
    setSelectedParticipantForRole(null);
    setRoleChangeAction(null);
  }, []);

  // Open recording dialog
  const openRecordingDialog = useCallback(() => {
    return new Promise((resolve) => {
      setRecordingDialogResolve(() => resolve);
      setShowRecordingDialog(true);
    });
  }, []);

  // Close recording dialog
  const closeRecordingDialog = useCallback((result) => {
    setShowRecordingDialog(false);
    if (recordingDialogResolve) {
      recordingDialogResolve(result);
      setRecordingDialogResolve(null);
    }
  }, [recordingDialogResolve]);

  // Open recording method dialog
  const openRecordingMethodDialog = useCallback(() => {
    setShowRecordingMethodDialog(true);
  }, []);

  // Close recording method dialog
  const closeRecordingMethodDialog = useCallback(() => {
    setShowRecordingMethodDialog(false);
  }, []);

  // ✅ NEW - Open screen share stopped dialog
  const openScreenShareStoppedDialog = useCallback((stoppedBy) => {
    setScreenShareStoppedBy(stoppedBy);
    setShowScreenShareStopped(true);
  }, []);

  // ✅ NEW - Close screen share stopped dialog
  const closeScreenShareStoppedDialog = useCallback(() => {
    setShowScreenShareStopped(false);
    setScreenShareStoppedBy(null);
  }, []);

  // Toggle meeting link popup
  const toggleMeetingLinkPopup = useCallback(() => {
    setShowMeetingLinkPopup(prev => !prev);
  }, []);

  // Minimize meeting link popup
  const minimizeMeetingLinkPopup = useCallback(() => {
    setMeetingLinkMinimized(true);
    setShowMeetingLinkPopup(false);
  }, []);

  // Restore meeting link popup
  const restoreMeetingLinkPopup = useCallback(() => {
    setMeetingLinkMinimized(false);
    setShowMeetingLinkPopup(true);
  }, []);

  // Close all dialogs
  const closeAllDialogs = useCallback(() => {
    setShowLeaveDialog(false);
    setShowEndMeetingDialog(false);
    setShowScreenShareRequest(false);
    setShowShareDialog(false);
    setShowCoHostDialog(false);
    setShowRecordingDialog(false);
    setShowRecordingMethodDialog(false);
    setShowScreenShareStopped(false); // ✅ NEW
    setScreenShareStoppedBy(null); // ✅ NEW
    setSelectedParticipantForRole(null);
    setRoleChangeAction(null);
  }, []);

  return {
    // States
    showLeaveDialog,
    showEndMeetingDialog,
    showScreenShareRequest,
    showShareDialog,
    showCoHostDialog,
    showRecordingDialog,
    showRecordingMethodDialog,
    showMeetingLinkPopup,
    meetingLinkMinimized,
    showScreenShareStopped, // ✅ NEW
    screenShareStoppedBy, // ✅ NEW
    selectedParticipantForRole,
    roleChangeAction,
    recordingDialogResolve,
    
    // Setters
    setShowLeaveDialog,
    setShowEndMeetingDialog,
    setShowScreenShareRequest,
    setShowShareDialog,
    setShowCoHostDialog,
    setShowRecordingDialog,
    setShowRecordingMethodDialog,
    setShowMeetingLinkPopup,
    setMeetingLinkMinimized,
    setShowScreenShareStopped, // ✅ NEW
    setScreenShareStoppedBy, // ✅ NEW
    setSelectedParticipantForRole,
    setRoleChangeAction,
    setRecordingDialogResolve,
    
    // Handlers
    openLeaveDialog,
    closeLeaveDialog,
    openEndMeetingDialog,
    closeEndMeetingDialog,
    openScreenShareRequestDialog,
    closeScreenShareRequestDialog,
    openShareDialog,
    closeShareDialog,
    openCoHostDialog,
    closeCoHostDialog,
    openRecordingDialog,
    closeRecordingDialog,
    openRecordingMethodDialog,
    closeRecordingMethodDialog,
    openScreenShareStoppedDialog, // ✅ NEW
    closeScreenShareStoppedDialog, // ✅ NEW
    toggleMeetingLinkPopup,
    minimizeMeetingLinkPopup,
    restoreMeetingLinkPopup,
    closeAllDialogs,
  };
};