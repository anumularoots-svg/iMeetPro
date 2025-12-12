// src/components/participants/ParticipantsList.jsx - FIXED VERSION WITH INTEGRATED CONTROLS
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import {
  Box,
  Typography,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Avatar,
  IconButton,
  Chip,
  Tooltip,
  Paper,
  Collapse,
  useTheme,
  alpha,
  Slide
} from '@mui/material';
import {
  Mic,
  MicOff,
  Videocam,
  VideocamOff,
  Search,
  People,
  Person,
  AdminPanelSettings,
  SupervisedUserCircle,
  KeyboardArrowDown,
  ScreenShare
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import ParticipantControls from './ParticipantControls';

// Enhanced styled components with professional design
const StyledPaper = styled(Paper)(({ theme }) => ({
  background: "linear-gradient(135deg, rgba(255, 255, 255, 0.95) 0%, rgba(248, 250, 252, 0.95) 100%)",
  backdropFilter: "blur(20px)",
  WebkitBackdropFilter: "blur(20px)",
  borderRadius: 20,
  padding: 0,
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.1)',
  border: '1px solid rgba(255, 255, 255, 0.2)',
  overflow: 'hidden',
  
  "& ::-webkit-scrollbar": {
    width: "8px",
  },
  "& ::-webkit-scrollbar-track": {
    background: "rgba(0, 0, 0, 0.05)",
    borderRadius: "4px",
  },
  "& ::-webkit-scrollbar-thumb": {
    background: "rgba(0, 0, 0, 0.2)",
    borderRadius: "4px",
    "&:hover": {
      background: "rgba(0, 0, 0, 0.3)",
    },
  },
}));

const HeaderBox = styled(Box)(({ theme }) => ({
  background: "linear-gradient(135deg, rgba(255, 255, 255, 0.9) 0%, rgba(248, 250, 252, 0.9) 100%)",
  backdropFilter: "blur(10px)",
  WebkitBackdropFilter: "blur(10px)",
  padding: '20px 24px',
  borderBottom: '1px solid rgba(0, 0, 0, 0.08)',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  position: 'relative',
  
  '&::after': {
    content: '""',
    position: 'absolute',
    bottom: 0,
    left: 0,
    right: 0,
    height: '1px',
    background: 'linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.1) 50%, transparent 100%)',
  }
}));

const SearchBox = styled(Box)(({ theme }) => ({
  background: "rgba(255, 255, 255, 0.7)",
  backdropFilter: "blur(10px)",
  WebkitBackdropFilter: "blur(10px)",
  padding: '16px 24px',
  borderBottom: '1px solid rgba(0, 0, 0, 0.05)',
  
  '& .search-container': {
    background: 'rgba(255, 255, 255, 0.8)',
    borderRadius: 16,
    border: '2px solid transparent',
    padding: '12px 16px',
    display: 'flex',
    alignItems: 'center',
    gap: 12,
    transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
    
    '&:focus-within': {
      background: 'rgba(255, 255, 255, 0.95)',
      borderColor: '#1976d2',
      boxShadow: '0 0 0 4px rgba(25, 118, 210, 0.1)',
      transform: 'translateY(-1px)',
    },
    
    '&:hover': {
      background: 'rgba(255, 255, 255, 0.9)',
      transform: 'translateY(-1px)',
    }
  },
  
  '& .search-input': {
    border: 'none',
    outline: 'none',
    background: 'transparent',
    fontSize: '14px',
    fontWeight: 500,
    color: '#1a1a1a',
    width: '100%',
    
    '&::placeholder': {
      color: 'rgba(26, 26, 26, 0.6)',
      fontSize: '14px',
    }
  }
}));

const SectionHeader = styled(Box)(({ theme }) => ({
  background: "linear-gradient(135deg, rgba(255, 255, 255, 0.8) 0%, rgba(248, 250, 252, 0.8) 100%)",
  margin: '8px 16px',
  borderRadius: 12,
  padding: '12px 16px',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  cursor: 'pointer',
  transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
  border: '1px solid rgba(0, 0, 0, 0.05)',
  
  '&:hover': {
    background: "linear-gradient(135deg, rgba(255, 255, 255, 0.95) 0%, rgba(248, 250, 252, 0.95) 100%)",
    transform: 'translateY(-1px)',
    boxShadow: '0 4px 20px rgba(0, 0, 0, 0.08)',
  },
  
  '&:active': {
    transform: 'translateY(0)',
  }
}));

const ParticipantContainer = styled(Box)(({ theme, isCurrentUser, userRole, isRemoving }) => ({
  background: isCurrentUser 
    ? "linear-gradient(135deg, rgba(25, 118, 210, 0.08) 0%, rgba(25, 118, 210, 0.04) 100%)"
    : userRole === 'host'
    ? "linear-gradient(135deg, rgba(255, 152, 0, 0.08) 0%, rgba(255, 152, 0, 0.04) 100%)"
    : userRole === 'co-host'
    ? "linear-gradient(135deg, rgba(255, 87, 34, 0.08) 0%, rgba(255, 87, 34, 0.04) 100%)"
    : "rgba(255, 255, 255, 0.7)",
  
  backdropFilter: "blur(10px)",
  WebkitBackdropFilter: "blur(10px)",
  margin: '0 16px 12px 16px',
  borderRadius: 16,
  border: isCurrentUser
    ? '2px solid rgba(25, 118, 210, 0.3)'
    : userRole === 'host'
    ? '2px solid rgba(255, 152, 0, 0.3)'
    : userRole === 'co-host'
    ? '2px solid rgba(255, 87, 34, 0.3)'
    : '1px solid rgba(0, 0, 0, 0.08)',
  
  overflow: 'hidden',
  transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
  opacity: isRemoving ? 0.5 : 1,
  filter: isRemoving ? 'grayscale(100%)' : 'none',
  position: 'relative',
  
  '&::before': isCurrentUser ? {
    content: '""',
    position: 'absolute',
    top: 0,
    left: 0,
    width: '4px',
    height: '100%',
    background: 'linear-gradient(180deg, #1976d2 0%, #42a5f5 100%)',
    borderRadius: '0 4px 4px 0',
  } : userRole === 'host' ? {
    content: '""',
    position: 'absolute',
    top: 0,
    left: 0,
    width: '4px',
    height: '100%',
    background: 'linear-gradient(180deg, #ff9800 0%, #ffb74d 100%)',
    borderRadius: '0 4px 4px 0',
  } : userRole === 'co-host' ? {
    content: '""',
    position: 'absolute',
    top: 0,
    left: 0,
    width: '4px',
    height: '100%',
    background: 'linear-gradient(180deg, #ff5722 0%, #ff8a65 100%)',
    borderRadius: '0 4px 4px 0',
  } : {},
}));

const ParticipantItem = styled(ListItem)(({ theme }) => ({
  padding: '16px 20px',
  minHeight: 'auto',
  alignItems: 'flex-start',
  background: 'transparent',
}));

const StatusIndicator = styled(Box)(({ theme, status }) => ({
  width: 10,
  height: 10,
  borderRadius: '50%',
  backgroundColor: 
    status === 'live' ? '#4caf50' :
    status === 'connecting' ? '#ff9800' :
    status === 'online' ? '#2196f3' : '#9e9e9e',
  marginRight: 8,
  marginTop: 6,
  boxShadow: status === 'live' 
    ? '0 0 8px rgba(76, 175, 80, 0.6)' 
    : status === 'connecting'
    ? '0 0 8px rgba(255, 152, 0, 0.6)'
    : 'none',
  animation: status === 'live' ? 'pulse 2s infinite' : 'none',
  
  '@keyframes pulse': {
    '0%': { transform: 'scale(1)', opacity: 1 },
    '50%': { transform: 'scale(1.2)', opacity: 0.8 },
    '100%': { transform: 'scale(1)', opacity: 1 },
  },
}));

const RoleChip = styled(Chip)(({ theme, role }) => ({
  height: 24,
  fontSize: '11px',
  fontWeight: 700,
  textTransform: 'uppercase',
  letterSpacing: 0.5,
  borderRadius: 12,
  
  ...(role === 'host' && {
    background: 'linear-gradient(135deg, rgba(255, 152, 0, 0.9) 0%, rgba(255, 152, 0, 0.8) 100%)',
    color: '#ffffff',
    border: 'none',
    boxShadow: '0 2px 8px rgba(255, 152, 0, 0.3)',
  }),
  
  ...(role === 'co-host' && {
    background: 'linear-gradient(135deg, rgba(255, 87, 34, 0.9) 0%, rgba(255, 87, 34, 0.8) 100%)',
    color: '#ffffff',
    border: 'none',
    boxShadow: '0 2px 8px rgba(255, 87, 34, 0.3)',
  }),
  
  ...(role === 'participant' && {
    background: 'linear-gradient(135deg, rgba(33, 150, 243, 0.9) 0%, rgba(33, 150, 243, 0.8) 100%)',
    color: '#ffffff',
    border: 'none',
    boxShadow: '0 2px 8px rgba(33, 150, 243, 0.3)',
  })
}));

const ActionButton = styled(IconButton)(({ theme, active }) => ({
  width: 36,
  height: 36,
  background: active 
    ? 'linear-gradient(135deg, #4caf50 0%, #66bb6a 100%)'
    : 'rgba(0, 0, 0, 0.04)',
  color: active ? '#ffffff' : 'rgba(0, 0, 0, 0.6)',
  border: 'none',
  borderRadius: 10,
  transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
  transformOrigin: 'center center',
  
  '&:hover': {
    background: active 
      ? 'linear-gradient(135deg, #388e3c 0%, #4caf50 100%)'
      : 'rgba(0, 0, 0, 0.08)',
    transform: 'scale(1.05)',
    boxShadow: active 
      ? '0 4px 16px rgba(76, 175, 80, 0.3)'
      : '0 2px 8px rgba(0, 0, 0, 0.1)',
  },
  
  '&:active': {
    transform: 'scale(0.95)',
  },
  
  '& .MuiSvgIcon-root': {
    fontSize: 18,
    transition: 'none',
  }
}));

const ParticipantsList = ({ 
  participants = [],
  currentUserId, 
  isHost = false,
  isCoHost = false,
  hasHostPrivileges = false,
  coHosts = [],
  onMuteParticipant,
  onUnmuteParticipant,
  onMuteVideo,
  onUnmuteVideo,
  onRemoveParticipant,
  onPromoteToCoHost,
  onRemoveCoHost,
  onDemoteParticipant,
  onSetVolume,
  onSpotlight,
  onPin,
  onBlock,
  onParticipantsUpdated,
  onPanelClose
}) => {
  const theme = useTheme();
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedSections, setExpandedSections] = useState({
    live: true,
    host: true,
    coHosts: true,
    participants: true,
    offline: false
  });
  const [removedParticipants, setRemovedParticipants] = useState(new Set());
  const [nameResolutionCache, setNameResolutionCache] = useState(new Map());

  const safeParticipants = Array.isArray(participants) ? participants : [];

  const getParticipantDisplayName = useCallback((participant) => {
    if (!participant) return 'Unknown User';
    
    const participantId = participant.id || participant.user_id || participant.User_ID || participant.ID;
    
    if (nameResolutionCache.has(participantId)) {
      const cachedName = nameResolutionCache.get(participantId);
      if (cachedName && cachedName !== 'Unknown User') {
        return cachedName;
      }
    }
    
    const nameFields = [
      participant.full_name,
      participant.Full_Name,
      participant.name,
      participant.displayName,
      participant.user_name,
      participant.username,
      participant.participant_name,
      participant.userName,
      participant.display_name,
      participant.fullName
    ];
    
    for (const nameField of nameFields) {
      if (nameField && 
          typeof nameField === 'string' && 
          nameField.trim() !== '' &&
          nameField !== 'Unknown' &&
          nameField !== 'Anonymous' &&
          nameField !== 'undefined' &&
          nameField !== 'null' &&
          !nameField.startsWith('User_') && 
          !nameField.startsWith('User ') &&
          !nameField.match(/^User\s*\d+$/) &&
          !nameField.match(/^participant_\d+$/)) {
        
        const cleanName = nameField.trim();
        setNameResolutionCache(prev => new Map(prev.set(participantId, cleanName)));
        return cleanName;
      }
    }
    
    const fallbackName = `User ${participantId || 'Unknown'}`;
    return fallbackName;
  }, [nameResolutionCache]);

  const getParticipantRole = (participant) => {
    if (!participant) return 'participant';
    
    if (participant.role === 'host' || participant.Role === 'host' || participant.isHost === true) {
      return 'host';
    }
    
    const participantId = participant.user_id || participant.User_ID || participant.id;
    const isParticipantCoHost = coHosts.some(cohost => {
      const cohostId = cohost.user_id || cohost.User_ID;
      return cohostId?.toString() === participantId?.toString();
    });
    
    if (participant.role === 'co-host' || participant.Role === 'co-host' || 
        participant.isCoHost === true || isParticipantCoHost) {
      return 'co-host';
    }
    
    return 'participant';
  };

  const getParticipantStatus = (participant) => {
    if (!participant) return 'offline';
    
    if (participant.Status) {
      return participant.Status;
    }
    
    if (participant.LiveKit_Connected === true) {
      return 'live';
    }
    
    if (!participant.Leave_Time && participant.Join_Time) {
      return 'connecting';
    }
    
    return 'offline';
  };

  const filteredParticipants = useMemo(() => {
    return safeParticipants.filter(participant => {
      if (!participant) return false;
      
      const participantId = participant.user_id || participant.User_ID || participant.id;
      if (removedParticipants.has(participantId)) return false;
      if (participant.Leave_Time) return false;
      
      const status = getParticipantStatus(participant);
      if (status === 'removed' || status === 'disconnected' || status === 'left' || status === 'offline') {
        return false;
      }
      
      if (searchTerm && searchTerm.trim() !== '') {
        const name = getParticipantDisplayName(participant);
        const email = participant.email || participant.Email || '';
        const searchLower = searchTerm.toLowerCase();
        
        const matchesSearch = name.toLowerCase().includes(searchLower) ||
                             email.toLowerCase().includes(searchLower);
        
        if (!matchesSearch) return false;
      }
      
      return true;
    });
  }, [safeParticipants, searchTerm, removedParticipants, getParticipantDisplayName, getParticipantStatus]);

  const groupedParticipants = {
    live: filteredParticipants.filter(p => getParticipantStatus(p) === 'live'),
    host: filteredParticipants.filter(p => getParticipantRole(p) === 'host'),
    coHosts: filteredParticipants.filter(p => getParticipantRole(p) === 'co-host'),
    participants: filteredParticipants.filter(p => getParticipantRole(p) === 'participant' && getParticipantStatus(p) !== 'offline'),
    offline: filteredParticipants.filter(p => getParticipantStatus(p) === 'offline')
  };

  const toggleSection = (section) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }));
  };

  const renderParticipantItem = (participant) => {
    if (!participant) return null;

    const participantId = participant.user_id || participant.User_ID || participant.id;
    const isCurrentUser = participantId && (
      participantId == currentUserId || 
      participant.isLocal === true
    );
    
    const participantRole = getParticipantRole(participant);
    const participantStatus = getParticipantStatus(participant);
    const isRemoving = removedParticipants.has(participantId);

    const uniqueKey = participant.key || 
                     participant.participant_id || 
                     participantId || 
                     participant.connection_id || 
                     `participant-${Math.random()}`;

    const displayName = getParticipantDisplayName(participant);

    return (
      <Slide 
        direction="up" 
        in={!isRemoving} 
        key={uniqueKey}
        timeout={300}
        mountOnEnter 
        unmountOnExit
      >
        <ParticipantContainer 
          isCurrentUser={isCurrentUser}
          userRole={participantRole}
          isRemoving={isRemoving}
        >
          <ParticipantItem>
            <ListItemAvatar sx={{ minWidth: 56 }}>
              <Box sx={{ position: 'relative' }}>
                <Avatar
                  src={participant.profile_picture || participant.avatar}
                  sx={{
                    width: 48,
                    height: 48,
                    fontSize: '18px',
                    fontWeight: 700,
                    background: participantRole === 'host' 
                      ? 'linear-gradient(135deg, #ff9800 0%, #f57c00 100%)'
                      : participantRole === 'co-host'
                      ? 'linear-gradient(135deg, #ff5722 0%, #d84315 100%)'
                      : 'linear-gradient(135deg, #2196f3 0%, #1976d2 100%)',
                    color: 'white',
                    border: '3px solid rgba(255, 255, 255, 0.9)',
                    boxShadow: '0 4px 16px rgba(0, 0, 0, 0.15)',
                  }}
                >
                  {displayName.charAt(0).toUpperCase()}
                </Avatar>
                <StatusIndicator 
                  status={participantStatus}
                  sx={{
                    position: 'absolute',
                    bottom: 2,
                    right: 2,
                    border: '2px solid white',
                  }}
                />
              </Box>
            </ListItemAvatar>

            <ListItemText
              primary={
                <Box display="flex" alignItems="center" justifyContent="space-between" mb={0.5}>
                  <Box display="flex" alignItems="center" gap={1}>
                    <Typography 
                      variant="body1" 
                      fontWeight={600}
                      sx={{ 
                        color: '#1a1a1a',
                        fontSize: '16px',
                        textOverflow: 'ellipsis',
                        overflow: 'hidden',
                        whiteSpace: 'nowrap',
                        maxWidth: '180px'
                      }}
                    >
                      {displayName}
                      {isCurrentUser && (
                        <span style={{ 
                          color: '#1976d2', 
                          fontWeight: 700,
                          marginLeft: '6px'
                        }}>
                          (You)
                        </span>
                      )}
                    </Typography>
                  </Box>
                  <RoleChip 
                    label={participantRole === 'co-host' ? 'Co-Host' : participantRole} 
                    size="small" 
                    role={participantRole}
                  />
                </Box>
              }
              secondary={
                <Box display="flex" alignItems="center" justifyContent="space-between" mt={1}>
                  <Typography 
                    variant="caption" 
                    sx={{ 
                      color: 'rgba(26, 26, 26, 0.7)',
                      fontSize: '13px',
                      fontWeight: 500,
                      textOverflow: 'ellipsis',
                      overflow: 'hidden',
                      whiteSpace: 'nowrap',
                      maxWidth: '140px'
                    }}
                  >
                    {participant.email || participant.Email || 'No email'}
                  </Typography>
                  
                  <Box display="flex" alignItems="center" gap={1}>
                    <Tooltip title={participant.audio_enabled || participant.isAudioEnabled ? "Microphone on" : "Microphone off"}>
                      <ActionButton 
                        size="small"
                        active={participant.audio_enabled || participant.isAudioEnabled}
                        disabled={isRemoving}
                      >
                        {(participant.audio_enabled || participant.isAudioEnabled) ? <Mic /> : <MicOff />}
                      </ActionButton>
                    </Tooltip>

                    <Tooltip title={participant.video_enabled || participant.isVideoEnabled ? "Camera on" : "Camera off"}>
                      <ActionButton 
                        size="small"
                        active={participant.video_enabled || participant.isVideoEnabled}
                        disabled={isRemoving}
                      >
                        {(participant.video_enabled || participant.isVideoEnabled) ? <Videocam /> : <VideocamOff />}
                      </ActionButton>
                    </Tooltip>

                    {participant.isScreenSharing && (
                      <Tooltip title="Screen sharing">
                        <ActionButton size="small" active={true}>
                          <ScreenShare />
                        </ActionButton>
                      </Tooltip>
                    )}

                    {!isCurrentUser && !isRemoving && (
                      <ParticipantControls
                        participant={participant}
                        currentUserRole={isHost ? 'host' : isCoHost ? 'co-host' : 'participant'}
                        isCurrentUser={isCurrentUser}
                        onMuteParticipant={onMuteParticipant}
                        onUnmuteParticipant={onUnmuteParticipant}
                        onMuteVideo={onMuteVideo}
                        onUnmuteVideo={onUnmuteVideo}
                        onRemoveParticipant={onRemoveParticipant}
                        onPromoteToCoHost={onPromoteToCoHost}
                        onDemoteFromCoHost={onRemoveCoHost}
                        onSetVolume={onSetVolume}
                        onSpotlight={onSpotlight}
                        onPin={onPin}
                        onBlock={onBlock}
                      />
                    )}
                  </Box>
                </Box>
              }
            />
          </ParticipantItem>
        </ParticipantContainer>
      </Slide>
    );
  };

  const renderSection = (title, participantsList, sectionKey, icon) => {
    const safeParticipantsList = Array.isArray(participantsList) ? participantsList : [];
    
    if (safeParticipantsList.length === 0) return null;

    return (
      <Box key={sectionKey}>
        <SectionHeader onClick={() => toggleSection(sectionKey)}>
          <Box display="flex" alignItems="center" gap={2}>
            {icon}
            <Typography 
              variant="subtitle2" 
              fontWeight={700}
              sx={{ 
                color: '#1a1a1a',
                fontSize: '14px',
                textTransform: 'uppercase',
                letterSpacing: 1
              }}
            >
              {title} ({safeParticipantsList.length})
            </Typography>
          </Box>
          <Box sx={{
            transform: expandedSections[sectionKey] ? 'rotate(180deg)' : 'rotate(0deg)',
            transition: 'transform 0.3s ease'
          }}>
            <KeyboardArrowDown sx={{ color: '#1a1a1a', fontSize: 22 }} />
          </Box>
        </SectionHeader>

        <Collapse in={expandedSections[sectionKey]} timeout={300}>
          <Box sx={{ pb: 1 }}>
            {safeParticipantsList.map(renderParticipantItem)}
          </Box>
        </Collapse>
      </Box>
    );
  };

  return (
    <Box sx={{ position: 'relative', height: '100%' }}>
      <StyledPaper>
        <HeaderBox>
          <Box>
            <Typography 
              variant="h6" 
              fontWeight={700}
              sx={{ 
                color: '#1a1a1a',
                fontSize: '20px'
              }}
            >
              Participants
            </Typography>
            <Typography 
              variant="body2" 
              sx={{ 
                color: 'rgba(26, 26, 26, 0.7)',
                fontSize: '14px',
                fontWeight: 500
              }}
            >
              {filteredParticipants.length} active
            </Typography>
          </Box>
        </HeaderBox>

        <SearchBox>
          <Box className="search-container">
            <Search sx={{ color: 'rgba(26, 26, 26, 0.7)', fontSize: 22 }} />
            <input
              className="search-input"
              placeholder="Search participants..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value || '')}
            />
          </Box>
        </SearchBox>

        <Box sx={{ flex: 1, overflowY: 'auto', pb: 2 }}>
          {renderSection(
            'Host',
            groupedParticipants.host,
            'host',
            <AdminPanelSettings sx={{ color: '#ff9800', fontSize: 18 }} />
          )}

          {renderSection(
            'Co-Hosts',
            groupedParticipants.coHosts,
            'coHosts',
            <SupervisedUserCircle sx={{ color: '#ff5722', fontSize: 18 }} />
          )}

          {renderSection(
            'Participants',
            groupedParticipants.participants,
            'participants',
            <Person sx={{ color: '#2196f3', fontSize: 18 }} />
          )}

          {filteredParticipants.length === 0 && (
            <Box 
              display="flex" 
              flexDirection="column" 
              alignItems="center" 
              justifyContent="center"
              py={8}
              sx={{ 
                background: 'rgba(255, 255, 255, 0.8)', 
                margin: 2, 
                borderRadius: 3,
                backdropFilter: 'blur(10px)',
                WebkitBackdropFilter: 'blur(10px)'
              }}
            >
              <People sx={{ fontSize: 64, color: 'rgba(26, 26, 26, 0.3)', mb: 3 }} />
              <Typography 
                variant="h6"
                sx={{ 
                  color: 'rgba(26, 26, 26, 0.7)',
                  fontSize: '18px',
                  textAlign: 'center',
                  fontWeight: 600,
                  mb: 1
                }}
              >
                {searchTerm ? 'No participants found' : 'No participants yet'}
              </Typography>
              {!searchTerm && (
                <Typography 
                  variant="body2" 
                  sx={{ 
                    color: 'rgba(26, 26, 26, 0.5)',
                    fontSize: '14px',
                    textAlign: 'center',
                    fontWeight: 500
                  }}
                >
                  Participants will appear here when they join
                </Typography>
              )}
            </Box>
          )}
        </Box>
      </StyledPaper>
    </Box>
  );
};

export default ParticipantsList;