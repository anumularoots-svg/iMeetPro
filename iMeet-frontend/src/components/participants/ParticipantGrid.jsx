import React, { useState, useEffect, useMemo } from 'react';
import {
  Box,
  Typography,
  IconButton,
  Tooltip,
  ToggleButton,
  ToggleButtonGroup,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  useTheme,
  alpha,
  Zoom,
  Fade
} from '@mui/material';
import {
  ViewModule,
  ViewList,
  ViewComfy,
  ViewQuilt,
  Fullscreen,
  FullscreenExit,
  Settings,
  AspectRatio,
  GridView,
  ViewStream,
  PictureInPicture,
  CenterFocusStrong,
  ViewAgenda
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import ParticipantReaction from './ParticipantReaction';

const GridContainer = styled(Box)(({ theme }) => ({
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  background: `linear-gradient(135deg, ${theme.palette.background.default} 0%, ${alpha(theme.palette.primary.main, 0.02)} 100%)`,
  borderRadius: theme.spacing(2),
  overflow: 'hidden',
}));

const GridHeader = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  padding: theme.spacing(1, 2),
  backgroundColor: alpha(theme.palette.background.paper, 0.8),
  backdropFilter: 'blur(10px)',
  borderBottom: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
}));

const GridContent = styled(Box)(({ theme, layout, participantCount }) => {
  const getGridConfig = () => {
    switch (layout) {
      case 'gallery':
        if (participantCount <= 4) return { columns: 2, rows: 2 };
        if (participantCount <= 9) return { columns: 3, rows: 3 };
        if (participantCount <= 16) return { columns: 4, rows: 4 };
        return { columns: 5, rows: 4 };
      
      case 'speaker':
        return { columns: 1, rows: 1 };
      
      case 'strip':
        return { columns: Math.min(participantCount, 6), rows: 1 };
      
      case 'sidebar':
        return { columns: 4, rows: 3 };
      
      case 'focus':
        return { columns: 2, rows: 2 };
      
      default:
        return { columns: 3, rows: 3 };
    }
  };

  const { columns, rows } = getGridConfig();

  return {
    flex: 1,
    display: 'grid',
    gridTemplateColumns: `repeat(${columns}, 1fr)`,
    gridTemplateRows: layout === 'strip' ? '1fr' : `repeat(${Math.min(rows, Math.ceil(participantCount / columns))}, 1fr)`,
    gap: theme.spacing(1.5),
    padding: theme.spacing(1.5),
    overflow: 'auto',
    '&::-webkit-scrollbar': {
      width: 8,
      height: 8,
    },
    '&::-webkit-scrollbar-track': {
      backgroundColor: alpha(theme.palette.grey[300], 0.3),
      borderRadius: 4,
    },
    '&::-webkit-scrollbar-thumb': {
      backgroundColor: alpha(theme.palette.primary.main, 0.5),
      borderRadius: 4,
      '&:hover': {
        backgroundColor: alpha(theme.palette.primary.main, 0.7),
      },
    },
  };
});

const SpotlightContainer = styled(Box)(({ theme }) => ({
  position: 'relative',
  gridColumn: 'span 2',
  gridRow: 'span 2',
  border: `3px solid ${theme.palette.warning.main}`,
  borderRadius: theme.spacing(2),
  background: `linear-gradient(135deg, ${alpha(theme.palette.warning.main, 0.1)} 0%, ${alpha(theme.palette.warning.main, 0.05)} 100%)`,
  '&::before': {
    content: '"SPOTLIGHT"',
    position: 'absolute',
    top: theme.spacing(1),
    left: theme.spacing(1),
    backgroundColor: theme.palette.warning.main,
    color: 'white',
    padding: theme.spacing(0.5, 1),
    borderRadius: theme.spacing(1),
    fontSize: '0.7rem',
    fontWeight: 600,
    zIndex: 10,
  },
}));

const PinnedContainer = styled(Box)(({ theme }) => ({
  position: 'relative',
  border: `2px solid ${theme.palette.info.main}`,
  borderRadius: theme.spacing(2),
  '&::before': {
    content: '"PINNED"',
    position: 'absolute',
    top: theme.spacing(1),
    right: theme.spacing(1),
    backgroundColor: theme.palette.info.main,
    color: 'white',
    padding: theme.spacing(0.5, 1),
    borderRadius: theme.spacing(1),
    fontSize: '0.7rem',
    fontWeight: 600,
    zIndex: 10,
  },
}));

const ParticipantGrid = ({
  participants = [],
  currentUserId,
  videoStreams = {},
  audioLevels = {},
  connectionQualities = {},
  onParticipantAction,
  showControls = true,
  allowLayoutChange = true,
  spotlightedParticipant = null,
  pinnedParticipants = [],
  maxVisibleParticipants = 25
}) => {
  const theme = useTheme();
  const [layout, setLayout] = useState('gallery');
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [settingsAnchor, setSettingsAnchor] = useState(null);
  const [cardSize, setCardSize] = useState('medium');
  const [showNames, setShowNames] = useState(true);
  const [autoLayout, setAutoLayout] = useState(true);

  // Auto-adjust layout based on participant count
  useEffect(() => {
    if (!autoLayout) return;
    
    const count = participants.length;
    if (count === 1) {
      setLayout('speaker');
    } else if (count <= 4) {
      setLayout('gallery');
    } else if (count <= 6) {
      setLayout('strip');
    } else {
      setLayout('gallery');
    }
  }, [participants.length, autoLayout]);

  // Sort and filter participants
  const sortedParticipants = useMemo(() => {
    let sorted = [...participants];
    
    // Sort by priority: spotlight > pinned > host > co-host > speaking > others
    sorted.sort((a, b) => {
      // Spotlight first
      if (spotlightedParticipant === a.id) return -1;
      if (spotlightedParticipant === b.id) return 1;
      
      // Pinned second
      const aPinned = pinnedParticipants.includes(a.id);
      const bPinned = pinnedParticipants.includes(b.id);
      if (aPinned && !bPinned) return -1;
      if (!aPinned && bPinned) return 1;
      
      // Host third
      if (a.role === 'host' && b.role !== 'host') return -1;
      if (a.role !== 'host' && b.role === 'host') return 1;
      
      // Co-host fourth
      if (a.role === 'co-host' && b.role === 'participant') return -1;
      if (a.role === 'participant' && b.role === 'co-host') return 1;
      
      // Speaking participants fifth
      const aSpeaking = audioLevels[a.id] > 0.1;
      const bSpeaking = audioLevels[b.id] > 0.1;
      if (aSpeaking && !bSpeaking) return -1;
      if (!aSpeaking && bSpeaking) return 1;
      
      // Current user sixth
      if (a.user_id === currentUserId) return -1;
      if (b.user_id === currentUserId) return 1;
      
      // Finally by join time
      return new Date(a.join_time) - new Date(b.join_time);
    });
    
    return sorted.slice(0, maxVisibleParticipants);
  }, [participants, spotlightedParticipant, pinnedParticipants, audioLevels, currentUserId, maxVisibleParticipants]);

  const handleLayoutChange = (event, newLayout) => {
    if (newLayout !== null) {
      setLayout(newLayout);
      setAutoLayout(false);
    }
  };

  const toggleFullscreen = () => {
    setIsFullscreen(!isFullscreen);
  };

  const renderParticipant = (participant, index) => {
    const isSpotlighted = spotlightedParticipant === participant.id;
    const isPinned = pinnedParticipants.includes(participant.id);
    const isCurrentUser = participant.user_id === currentUserId;
    
    const ParticipantElement = (
      <Box key={participant.id} sx={{ position: 'relative', height: '100%' }}>
        <ParticipantCard
          participant={participant}
          isCurrentUser={isCurrentUser}
          videoStream={videoStreams[participant.id]}
          audioLevel={audioLevels[participant.id] || 0}
          connectionQuality={connectionQualities[participant.id] || 'good'}
          onMuteToggle={onParticipantAction}
          onVideoToggle={onParticipantAction}
          showControls={showControls}
          size={cardSize}
        />
        <ParticipantReaction
          participant={participant}
          position="overlay"
          showFloatingReactions={true}
          showPersistentReactions={true}
        />
      </Box>
    );

    if (isSpotlighted) {
      return (
        <SpotlightContainer key={participant.id}>
          {ParticipantElement}
        </SpotlightContainer>
      );
    }

    if (isPinned) {
      return (
        <PinnedContainer key={participant.id}>
          {ParticipantElement}
        </PinnedContainer>
      );
    }

    return ParticipantElement;
  };

  return (
    <GridContainer>
      {/* Header Controls */}
      <GridHeader>
        <Box display="flex" alignItems="center" gap={1}>
          <Typography variant="h6" fontWeight={600}>
            Participants ({participants.length})
          </Typography>
          {participants.length > maxVisibleParticipants && (
            <Typography variant="caption" color="text.secondary">
              Showing {maxVisibleParticipants} of {participants.length}
            </Typography>
          )}
        </Box>

        <Box display="flex" alignItems="center" gap={1}>
          {/* Layout Controls */}
          {allowLayoutChange && (
            <ToggleButtonGroup
              value={layout}
              exclusive
              onChange={handleLayoutChange}
              size="small"
              sx={{
                '& .MuiToggleButton-root': {
                  border: `1px solid ${alpha(theme.palette.primary.main, 0.3)}`,
                  '&.Mui-selected': {
                    backgroundColor: alpha(theme.palette.primary.main, 0.1),
                    color: theme.palette.primary.main,
                  },
                },
              }}
            >
              <ToggleButton value="gallery">
                <Tooltip title="Gallery View">
                  <ViewModule />
                </Tooltip>
              </ToggleButton>
              <ToggleButton value="speaker">
                <Tooltip title="Speaker View">
                  <CenterFocusStrong />
                </Tooltip>
              </ToggleButton>
              <ToggleButton value="strip">
                <Tooltip title="Strip View">
                  <ViewStream />
                </Tooltip>
              </ToggleButton>
              <ToggleButton value="sidebar">
                <Tooltip title="Sidebar View">
                  <ViewAgenda />
                </Tooltip>
              </ToggleButton>
            </ToggleButtonGroup>
          )}

          {/* Settings Menu */}
          <IconButton
            size="small"
            onClick={(e) => setSettingsAnchor(e.currentTarget)}
            sx={{
              backgroundColor: alpha(theme.palette.primary.main, 0.1),
              '&:hover': {
                backgroundColor: alpha(theme.palette.primary.main, 0.2),
              },
            }}
          >
            <Settings />
          </IconButton>

          {/* Fullscreen Toggle */}
          <IconButton
            size="small"
            onClick={toggleFullscreen}
            sx={{
              backgroundColor: alpha(theme.palette.secondary.main, 0.1),
              '&:hover': {
                backgroundColor: alpha(theme.palette.secondary.main, 0.2),
              },
            }}
          >
            {isFullscreen ? <FullscreenExit /> : <Fullscreen />}
          </IconButton>
        </Box>
      </GridHeader>

      {/* Grid Content */}
      <GridContent layout={layout} participantCount={sortedParticipants.length}>
        {sortedParticipants.length === 0 ? (
          <Box
            display="flex"
            flexDirection="column"
            alignItems="center"
            justifyContent="center"
            height="100%"
            gridColumn="1 / -1"
          >
            <ViewModule sx={{ fontSize: 64, color: theme.palette.grey[400], mb: 2 }} />
            <Typography variant="h6" color="text.secondary" textAlign="center">
              No participants to display
            </Typography>
            <Typography variant="body2" color="text.secondary" textAlign="center">
              Participants will appear here when they join the meeting
            </Typography>
          </Box>
        ) : (
          sortedParticipants.map((participant, index) => (
            <Zoom
              key={participant.id}
              in={true}
              timeout={300}
              style={{ transitionDelay: `${index * 100}ms` }}
            >
              <Box>
                {renderParticipant(participant, index)}
              </Box>
            </Zoom>
          ))
        )}
      </GridContent>

      {/* Settings Menu */}
      <Menu
        anchorEl={settingsAnchor}
        open={Boolean(settingsAnchor)}
        onClose={() => setSettingsAnchor(null)}
        PaperProps={{
          sx: {
            borderRadius: 2,
            minWidth: 200,
            boxShadow: theme.shadows[8],
          }
        }}
      >
        <MenuItem onClick={() => setCardSize(cardSize === 'small' ? 'medium' : cardSize === 'medium' ? 'large' : 'small')}>
          <ListItemIcon>
            <AspectRatio />
          </ListItemIcon>
          <ListItemText>
            Card Size: {cardSize}
          </ListItemText>
        </MenuItem>
        
        <MenuItem onClick={() => setShowNames(!showNames)}>
          <ListItemIcon>
            <ViewList />
          </ListItemIcon>
          <ListItemText>
            {showNames ? 'Hide Names' : 'Show Names'}
          </ListItemText>
        </MenuItem>
        
        <MenuItem onClick={() => setAutoLayout(!autoLayout)}>
          <ListItemIcon>
            <GridView />
          </ListItemIcon>
          <ListItemText>
            Auto Layout: {autoLayout ? 'On' : 'Off'}
          </ListItemText>
        </MenuItem>
      </Menu>
    </GridContainer>
  );
};

export default ParticipantGrid;