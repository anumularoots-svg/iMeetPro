import React, { useState, useEffect } from "react";
import {
  Box,
  AppBar,
  Toolbar,
  Typography,
  IconButton,
  Avatar,
  Menu,
  MenuItem,
  Badge,
  Tooltip,
  useTheme,
  useMediaQuery,
  Divider,
  ListItemIcon,
  ListItemText,
  ClickAwayListener,
} from "@mui/material";
import {
  Notifications,
  Settings,
  VideoCall,
  ExitToApp,
  Person,
  KeyboardArrowDown,
} from "@mui/icons-material";
import { useAuth } from "../hooks/useAuth";
import { useNavigate } from "react-router-dom";
import { useNotificationContext } from "../context/NotificationContext";
import NotificationDropdown from "../components/common/NotificationDropdown";

const MainLayout = ({ children }) => {
  const [anchorEl, setAnchorEl] = useState(null);
  const [notificationOpen, setNotificationOpen] = useState(false);
  const [logoError, setLogoError] = useState(false);
  const [profilePicture, setProfilePicture] = useState(null);
  const { user, logout } = useAuth();
  const { unreadCount = 0 } = useNotificationContext();
  const navigate = useNavigate();
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down("md"));

  // Update profile picture when user changes
  useEffect(() => {
    if (user) {
      console.log('🖼️ MainLayout: User data updated:', {
        userId: user.id || user.Id || user.ID,
        hasProfilePicture: !!user.profile_picture,
        picturePreview: user.profile_picture?.substring(0, 100)
      });
      
      // Support multiple field names from backend
      const picture = user.profile_picture || user.profile_photo || null;
      setProfilePicture(picture);
    }
  }, [user, user?.profile_picture, user?.profile_photo]);

  // Listen for profile picture updates from other components
  useEffect(() => {
    const handleProfileUpdate = (event) => {
      console.log('🔄 MainLayout: Profile picture update event received');
      if (event.detail?.profile_picture) {
        setProfilePicture(event.detail.profile_picture);
      }
    };

    window.addEventListener('profilePictureUpdated', handleProfileUpdate);
    
    return () => {
      window.removeEventListener('profilePictureUpdated', handleProfileUpdate);
    };
  }, []);

  const handleMenu = (event) => setAnchorEl(event.currentTarget);
  const handleClose = () => setAnchorEl(null);
  
  const handleNotificationToggle = () => {
    setNotificationOpen(!notificationOpen);
  };

  const handleNotificationClose = () => {
    setNotificationOpen(false);
  };

  const handleLogout = () => {
    logout();
    handleClose();
    navigate("/auth");
  };

  // Get user's first letter for avatar fallback
  const getUserInitial = () => {
    const name = user?.full_name || user?.name || user?.email || "U";
    return name.charAt(0).toUpperCase();
  };

  return (
    <>
      <Box
        sx={{
          flexGrow: 1,
          minHeight: "100vh",
          bgcolor: "background.default",
          position: "relative",
        }}
      >
        <AppBar
          position="fixed"
          elevation={0}
          sx={{
            bgcolor: "#3c6af6ff",
            color: "text.primary",
            borderBottom: "1px solid rgba(0, 0, 0, 0.08)",
            boxShadow: "0 1px 3px rgba(0, 0, 0, 0.05)",
          }}
        >
          <Toolbar
            sx={{
              minHeight: { xs: 64, sm: 70 },
              px: { xs: 2, sm: 9, md: 4 },
            }}
          >
            {/* Left Side - Logo/Branding */}
            <Box
              sx={{
                display: "flex",
                alignItems: "center",
              }}
            >
              {/* Logo */}
              {!logoError ? (
                <Box
                  component="img"
                  src="../../public/assests/images/IMeetPro.png"
                  alt="MeetPro Logo"
                  onError={() => setLogoError(true)}
                  sx={{
                    height: { xs: 42, sm: 82 },
                    width: "auto",
                    maxWidth: { xs: "120px", sm: "140px" },
                    objectFit: "contain",
                    borderRadius: "15px",
                    cursor: "pointer",
                  }}
                  onClick={() => navigate("/dashboard")}
                />
              ) : (
                <Box
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    gap: 1,
                    cursor: "pointer",
                    py: 1,
                    px: { xs: 1, sm: 2 },
                    borderRadius: 2,
                    "&:hover": {
                      bgcolor: "rgba(45, 91, 229, 0.05)",
                    },
                  }}
                  onClick={() => navigate("/dashboard")}
                >
                  <VideoCall
                    sx={{
                      fontSize: { xs: 28, sm: 32 },
                      color: "#2d5be5ff",
                    }}
                  />
                  <Typography
                    variant={isMobile ? "h6" : "h5"}
                    sx={{
                      color: "#3c6af6ff",
                      fontWeight: "bold",
                      letterSpacing: "0.5px",
                      fontSize: { xs: "1.1rem", sm: "1.3rem" },
                    }}
                  >
                    MeetPro
                  </Typography>
                </Box>
              )}
            </Box>

            {/* Spacer to push content to the right */}
            <Box sx={{ flexGrow: 1 }} />

            {/* Action Buttons */}
            <Box
              sx={{
                display: "flex",
                alignItems: "center",
                gap: { xs: 1, sm: 1.5 },
              }}
            >
              {/* Notifications */}
              <ClickAwayListener onClickAway={handleNotificationClose}>
                <Box sx={{ position: "relative" }}>
                  <Tooltip title="Notifications">
                    <IconButton
                      onClick={handleNotificationToggle}
                      color="inherit"
                      sx={{
                        bgcolor: "#3c6af6ff",
                        color: "#ffffff",
                        border: "1px solid rgba(0, 0, 0, 0.08)",
                        "&:hover": {
                          borderColor: "primary.main",
                          transform: "translateY(-1px)",
                        },
                        transition: "all 0.2s ease-in-out",
                      }}
                    >
                      <Badge
                        badgeContent={unreadCount}
                        sx={{
                          "& .MuiBadge-badge": {
                            bgcolor: "#EF4444",
                            color: "#ffffff",
                            fontSize: "0.7rem",
                            minWidth: "18px",
                            height: "18px",
                            boxShadow: "0 2px 8px rgba(239, 68, 68, 0.4)",
                          },
                        }}
                      >
                        <Notifications />
                      </Badge>
                    </IconButton>
                  </Tooltip>

                  <NotificationDropdown
                    open={notificationOpen}
                    onClose={handleNotificationClose}
                    filterType="all"
                  />
                </Box>
              </ClickAwayListener>

              {/* Profile Menu */}
              <Tooltip title="Profile Menu">
                <Box
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    color: "#ffffff",
                  }}
                >
                  <IconButton
                    onClick={handleMenu}
                    sx={{
                      p: 0.5,
                      bgcolor: "#3c6af6ff",
                      borderRadius: "12px",
                      "&:hover": {
                        bgcolor: "#3c6af6ff",
                        color: "#ffffff",
                      },
                      transition: "all 0.2s ease-in-out",
                    }}
                  >
                    <Box sx={{ display: "flex", alignItems: "center", px: 1 }}>
                      <Avatar
                        src={profilePicture}
                        sx={{
                          width: { xs: 32, sm: 36 },
                          height: { xs: 32, sm: 36 },
                          border: "2px solid rgba(255,255,255,0.5)",
                          mr: !isMobile ? 1 : 0,
                          bgcolor: profilePicture ? 'transparent' : theme.palette.primary.main,
                        }}
                      >
                        {!profilePicture && getUserInitial()}
                      </Avatar>
                    </Box>
                  </IconButton>
                </Box>
              </Tooltip>
            </Box>
          </Toolbar>
        </AppBar>

{/* Profile Menu */}
        <Menu
          anchorEl={anchorEl}
          open={Boolean(anchorEl)}
          onClose={handleClose}
          PaperProps={{
            elevation: 0,
            sx: {
              mt: 1.5,
              minWidth: 280,
              borderRadius: 2,
              overflow: 'visible',
              boxShadow: '0 8px 32px rgba(0, 0, 0, 0.12)',
              border: '1px solid',
              borderColor: 'divider',
              '&:before': {
                content: '""',
                display: 'block',
                position: 'absolute',
                top: 0,
                right: 14,
                width: 10,
                height: 10,
                bgcolor: 'background.paper',
                transform: 'translateY(-50%) rotate(45deg)',
                zIndex: 0,
                borderLeft: '1px solid',
                borderTop: '1px solid',
                borderColor: 'divider',
              },
            },
          }}
          transformOrigin={{ horizontal: 'right', vertical: 'top' }}
          anchorOrigin={{ horizontal: 'right', vertical: 'bottom' }}
        >
          {/* Profile Header */}
          <Box 
            sx={{ 
              p: 3,
              pb: 2.5,
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Avatar
                src={profilePicture}
                sx={{
                  width: 56,
                  height: 56,
                  border: '2px solid',
                  borderColor: 'divider',
                  bgcolor: profilePicture ? 'transparent' : 'primary.main',
                  boxShadow: '0 2px 8px rgba(0, 0, 0, 0.08)',
                  fontSize: '1.5rem',
                  fontWeight: 600,
                  color: '#ffffff',
                }}
              >
                {!profilePicture && getUserInitial()}
              </Avatar>
              <Box sx={{ flex: 1, minWidth: 0 }}>
                <Typography
                  variant="subtitle1"
                  sx={{ 
                    fontWeight: 600, 
                    color: 'text.primary',
                    lineHeight: 1.3,
                    mb: 0.5,
                    letterSpacing: '0.01em',
                  }}
                >
                  {user?.full_name || user?.name || 'User'}
                </Typography>
                <Typography
                  variant="caption"
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.813rem',
                    display: 'block',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {user?.email || 'user@example.com'}
                </Typography>
              </Box>
            </Box>
          </Box>

          <Divider />

          {/* Menu Items */}
          <Box sx={{ py: 1 }}>
            <MenuItem
              onClick={() => {
                navigate('/profile');
                handleClose();
              }}
              sx={{ 
                px: 3,
                py: 1.5,
                mx: 1,
                borderRadius: 1,
                transition: 'all 0.2s ease',
                '&:hover': {
                  bgcolor: 'action.hover',
                  transform: 'translateX(4px)',
                },
              }}
            >
              <ListItemIcon sx={{ minWidth: 40 }}>
                <Person fontSize="small" sx={{ color: 'text.secondary' }} />
              </ListItemIcon>
              <ListItemText
                primary={
                  <Typography variant="body2" sx={{ fontWeight: 500, color: 'text.primary' }}>
                    My Profile
                  </Typography>
                }
                secondary={
                  <Typography variant="caption" sx={{ color: 'text.secondary', fontSize: '0.75rem' }}>
                    View and edit profile
                  </Typography>
                }
              />
            </MenuItem>

            <MenuItem
              onClick={() => {
                navigate('/settings');
                handleClose();
              }}
              sx={{ 
                px: 3,
                py: 1.5,
                mx: 1,
                borderRadius: 1,
                transition: 'all 0.2s ease',
                '&:hover': {
                  bgcolor: 'action.hover',
                  transform: 'translateX(4px)',
                },
              }}
            >
              <ListItemIcon sx={{ minWidth: 40 }}>
                <Settings fontSize="small" sx={{ color: 'text.secondary' }} />
              </ListItemIcon>
              <ListItemText
                primary={
                  <Typography variant="body2" sx={{ fontWeight: 500, color: 'text.primary' }}>
                    Settings
                  </Typography>
                }
                secondary={
                  <Typography variant="caption" sx={{ color: 'text.secondary', fontSize: '0.75rem' }}>
                    App preferences
                  </Typography>
                }
              />
            </MenuItem>
          </Box>

          <Divider sx={{ my: 1 }} />

          {/* Sign Out */}
          <Box sx={{ pb: 1 }}>
            <MenuItem
              onClick={handleLogout}
              sx={{ 
                px: 3,
                py: 1.5,
                mx: 1,
                borderRadius: 1,
                transition: 'all 0.2s ease',
                '&:hover': {
                  bgcolor: 'error.lighter',
                  transform: 'translateX(4px)',
                },
              }}
            >
              <ListItemIcon sx={{ minWidth: 40 }}>
                <ExitToApp fontSize="small" sx={{ color: 'error.main' }} />
              </ListItemIcon>
              <ListItemText
                primary={
                  <Typography variant="body2" sx={{ fontWeight: 500, color: 'error.main' }}>
                    Sign Out
                  </Typography>
                }
                secondary={
                  <Typography variant="caption" sx={{ color: 'text.secondary', fontSize: '0.75rem' }}>
                    End current session
                  </Typography>
                }
              />
            </MenuItem>
          </Box>
        </Menu>

        {/* Main Content */}
        <Box
          component="main"
          sx={{
            mt: { xs: 8, sm: 8.75 },
            minHeight: { xs: "calc(100vh - 64px)", sm: "calc(100vh - 70px)" },
            bgcolor: "background.default",
          }}
        >
          {children}
        </Box>
      </Box>
    </>
  );
};

export default MainLayout;