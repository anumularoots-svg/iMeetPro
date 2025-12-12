// src/components/tabs/BrowserTabsHeader.jsx
import React from 'react';
import { Box, IconButton } from '@mui/material';
import { 
  Close, 
  VideoCall, 
  Gesture as WhiteboardIcon 
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';

const TabsContainer = styled(Box)(({ theme }) => ({
  position: 'fixed',
  top: 0,
  left: 0,
  right: 0,
  height: 42,
  display: 'flex',
  alignItems: 'flex-end',
  padding: '0 8px',
  zIndex: 1000,
}));

const BrowserTab = styled(Box, {
  shouldForwardProp: (prop) => !['active'].includes(prop)
})(({ theme, active }) => ({
  position: 'relative',
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(2),
  height: 36,
  minWidth: 160,
  maxWidth: 220,
  padding: '12px 12px',
  marginRight: '1px',
  background: active ? '#2a2d35' : 'rgba(255, 255, 255, 0.04)',
  color: active ? '#fff' : 'rgba(255, 255, 255, 0.55)',
  cursor: 'pointer',
  transition: 'all 0.15s ease',
  fontSize: '13px',
  fontWeight: 400,
  userSelect: 'none',
  borderTopLeftRadius: '8px',
  borderTopRightRadius: '8px',
  borderBottomLeftRadius: '8px',
  borderBottomRightRadius: "8px",
  border: active ? '1px solid rgba(255, 255, 255, 0.12)' : '1px solid transparent',
  borderBottom: active ? '1px solid #2a2d35' : 'none',

  '&:hover': {
    background: active ? '#2a2d35' : 'rgba(255, 255, 255, 0.07)',
    color: active ? '#fff' : 'rgba(255, 255, 255, 0.8)',
  },

  '& .tab-icon': {
    fontSize: 16,
    flexShrink: 0,
    opacity: active ? 1 : 0.7,
  },

  '& .tab-title': {
    flex: 1,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  }
}));

const TabCloseButton = styled(IconButton)(({ theme }) => ({
  width: 18,
  height: 18,
  padding: 0,
  marginLeft: theme.spacing(0.5),
  color: 'rgba(255, 255, 255, 0.4)',
  transition: 'all 0.15s',
  borderRadius: '4px',

  '&:hover': {
    color: 'rgba(255, 255, 255, 0.9)',
    background: 'rgba(255, 255, 255, 0.12)',
  },

  '& .MuiSvgIcon-root': {
    fontSize: 14,
  }
}));

const BrowserTabsHeader = ({ 
  availableTabs, 
  activeTab, 
  onTabChange, 
  onTabClose 
}) => {
  const getTabIcon = (tab) => {
    switch (tab) {
      case 'meeting':
        return <VideoCall className="tab-icon" />;
      case 'whiteboard':
        return <WhiteboardIcon className="tab-icon" />;
      default:
        return null;
    }
  };

  const getTabTitle = (tab) => {
    return tab.charAt(0).toUpperCase() + tab.slice(1);
  };

  return (
    <TabsContainer>
      <Box sx={{ display: 'flex', alignItems: 'flex-end', gap: 3, flex: 1, overflow: 'hidden' }}>
        {availableTabs.map((tab) => (
          <BrowserTab
            key={tab}
            active={activeTab === tab}
            onClick={() => onTabChange(tab)}
          >
            <Box className="tab-icon">
              {getTabIcon(tab)}
            </Box>
            <span className="tab-title">{getTabTitle(tab)}</span>

            {/* Close button - hide for meeting tab */}
            {tab !== 'meeting' && (
              <TabCloseButton
                onClick={(e) => {
                  e.stopPropagation();
                  onTabClose(tab);
                }}
              >
                <Close />
              </TabCloseButton>
            )}
          </BrowserTab>
        ))}
      </Box>
    </TabsContainer>
  );
};

export default BrowserTabsHeader;