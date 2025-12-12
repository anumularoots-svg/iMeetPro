import React, { useState } from "react";
import {
  Box,
  Drawer,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Divider,
  IconButton,
  useTheme,
  useMediaQuery,
  Collapse,
  Badge,
  Typography,
  Avatar,
  Tooltip,
} from "@mui/material";
import {
  VideoCall,
  Schedule,
  CalendarMonth,
  VideoLibrary,
  Analytics,
  Person,
  Dashboard as DashboardIcon,
  Settings,
  ExpandLess,
  ExpandMore,
  Menu as MenuIcon,
  AccessTime,
  Event,
  PlayCircle,
  Notifications,
  Close,
} from "@mui/icons-material";
import { useNavigate, useLocation } from "react-router-dom";

const drawerWidth = 280;

const DashboardLayout = ({ children, badges = {} }) => {
  const [mobileOpen, setMobileOpen] = useState(false);
  const [recordingsMenuOpen, setRecordingsMenuOpen] = useState(false);
  const [meetingMenuOpen, setMeetingMenuOpen] = useState(false);
  const [logoError, setLogoError] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down("md"));

  // Default badge values, can be overridden by props
  const defaultBadges = {
    schedule: 0,
    recordings: 0,
    notifications: 0,
    ...badges,
  };

  const menuItems = [
    {
      text: "Dashboard",
      icon: <DashboardIcon />,
      path: "/dashboard",
      badge: null,
    },
    {
      text: "New Meeting",
      icon: <VideoCall />,
      path: "/meeting/new",
      badge: null,
      subItems: [
        {
          text: "Instant Meeting",
          icon: <PlayCircle />,
          path: "/meeting/instant",
        },
        {
          text: "Schedule Meeting",
          icon: <AccessTime />,
          path: "/meeting/schedule",
        },
        {
          text: "Calendar Meeting",
          icon: <Event />,
          path: "/meeting/calendar",
        },
      ],
    },
    {
      text: "Schedule",
      icon: <Schedule />,
      path: "/schedule",
      badge: defaultBadges.schedule > 0 ? defaultBadges.schedule : null,
    },
    {
      text: "Calendar",
      icon: <CalendarMonth />,
      path: "/calendar",
      badge: null,
    },
    {
      text: "Recordings",
      icon: <VideoLibrary />,
      path: "/recordings",
      badge: defaultBadges.recordings > 0 ? defaultBadges.recordings : null,
      subItems: [
        // ADD THIS ENTIRE subItems ARRAY
        {
          text: "All Recordings",
          icon: <VideoLibrary />,
          path: "/recordings?type=all",
        },
        {
          text: "Instant Meetings",
          icon: <PlayCircle />,
          path: "/recordings?type=instant",
        },
        {
          text: "Scheduled Meetings",
          icon: <AccessTime />,
          path: "/recordings?type=scheduled",
        },
        {
          text: "Calendar Meetings",
          icon: <Event />,
          path: "/recordings?type=calendar",
        },
      ],
    },
    {
      text: "Analytics",
      icon: <Analytics />,
      path: "/analytics",
      badge: null,
    },
  ];

  const bottomItems = [
    { text: "Profile", icon: <Person />, path: "/profile" },
    { text: "Settings", icon: <Settings />, path: "/settings" },
  ];

  const handleDrawerToggle = () => {
    setMobileOpen(!mobileOpen);
  };

  const handleDrawerClose = () => {
    setMobileOpen(false);
  };

  const handleMenuClick = (item) => {
    if (item.subItems) {
      if (item.text === "New Meeting") {
        setMeetingMenuOpen(!meetingMenuOpen);
      } else if (item.text === "Recordings") {
        // ADD THIS
        setRecordingsMenuOpen(!recordingsMenuOpen);
      }
    } else {
      navigate(item.path);
      if (isMobile) {
        setMobileOpen(false);
      }
    }
  };

  const handleSubMenuClick = (subItem) => {
    console.log("🔘 Submenu clicked:", subItem.text, "path:", subItem.path);
    
    // Close submenus first
    setMeetingMenuOpen(false);
    setRecordingsMenuOpen(false);
    
    // Close mobile drawer
    if (isMobile) {
      setMobileOpen(false);
    }
    
    // Navigate using React Router
    navigate(subItem.path);
  };

  const isSelected = (path) => location.pathname === path;

  const drawer = (
    <Box
      sx={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        bgcolor: "#ffffff",
        position: "relative",
        overflow: "hidden",
      }}
    >
      {/* Subtle gradient overlay for depth */}
      <Box
        sx={{
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background:
            "linear-gradient(180deg, rgba(45, 91, 229, 0.02) 0%, rgba(45, 91, 229, 0.01) 100%)",
          zIndex: 0,
        }}
      />

      {/* Header with close button for mobile */}
      <Box
        sx={{
          p: isMobile ? 1 : 2,
          textAlign: "center",
          position: "relative",
          zIndex: 1,
          flexShrink: 0,
          bgcolor: "#3c6af6ff",
          background: "linear-gradient(135deg, #2d5be5ff 0%, #4c6ef5 100%)",
          borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
          mb: 1,
        }}
      >
        {/* Mobile close button */}
        {isMobile && (
          <IconButton
            onClick={handleDrawerClose}
            sx={{
              position: "absolute",
              top: 8,
              right: 8,
              color: "#ffffff",
              bgcolor: "rgba(255, 255, 255, 0.15)",
              width: 32,
              height: 32,
              "&:hover": {
                bgcolor: "rgba(255, 255, 255, 0.25)",
                color: "#ffffff",
                transform: "scale(1.05)",
              },
              transition: "all 0.2s ease-in-out",
              zIndex: 2,
            }}
          >
            <Close fontSize="small" />
          </IconButton>
        )}

        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            mb: isMobile ? 1 : 2,
            mt: isMobile ? 1 : 0,
            px: isMobile ? 2 : 0,
          }}
        >
          {!logoError ? (
            <Box
              component="img"
              src="../../public/assests/images/IMeetPro.png"
              alt="MeetPro Logo"
              onError={() => setLogoError(true)}
              sx={{
                height: isMobile ? 50 : 150,
                width: "auto",
                maxWidth: isMobile ? "140px" : "180px",
                objectFit: "contain",
                borderRadius: isMobile ? "6px" : "8px",
                filter:
                  "brightness(1.1) drop-shadow(0 2px 8px rgba(0,0,0,0.2))",
                transition: "transform 0.2s ease-in-out",
                "&:hover": {
                  transform: "scale(1.02)",
                },
              }}
            />
          ) : (
            // Fallback when image fails to load
            <Box
              sx={{
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                flexDirection: "column",
                py: isMobile ? 1 : 2,
              }}
            >
              <Box
                sx={{
                  p: 2,
                  borderRadius: "16px",
                  bgcolor: "rgba(255, 255, 255, 0.15)",
                  mb: 1,
                  boxShadow: "0 4px 12px rgba(0, 0, 0, 0.15)",
                  backdropFilter: "blur(10px)",
                }}
              >
                <VideoCall
                  sx={{
                    fontSize: isMobile ? 28 : 36,
                    color: "#ffffff",
                  }}
                />
              </Box>
              <Typography
                variant={isMobile ? "h6" : "h5"}
                sx={{
                  color: "#ffffff",
                  fontWeight: 700,
                  letterSpacing: "-0.025em",
                  fontSize: isMobile ? "1.1rem" : "1.3rem",
                  textShadow: "0 2px 4px rgba(0,0,0,0.2)",
                }}
              >
                MeetPro
              </Typography>
              <Typography
                variant="caption"
                sx={{
                  color: "rgba(255, 255, 255, 0.8)",
                  fontSize: "0.75rem",
                  fontWeight: 500,
                  textShadow: "0 1px 2px rgba(0,0,0,0.1)",
                }}
              >
                Video Conferencing
              </Typography>
            </Box>
          )}
        </Box>
      </Box>

      {/* Scrollable Content Area */}
      <Box
        sx={{
          flexGrow: 1,
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          position: "relative",
          zIndex: 1,
        }}
      >
        {/* Main Menu - Scrollable */}
        <Box
          sx={{
            flexGrow: 1,
            overflowY: "auto",
            overflowX: "hidden",
            px: 2,
            py: 1,
            "&::-webkit-scrollbar": {
              width: "4px",
            },
            "&::-webkit-scrollbar-track": {
              background: "transparent",
            },
            "&::-webkit-scrollbar-thumb": {
              background: "rgba(156, 163, 175, 0.4)",
              borderRadius: "2px",
              "&:hover": {
                background: "rgba(156, 163, 175, 0.6)",
              },
            },
          }}
        >
          <List sx={{ py: 0 }}>
            {menuItems.map((item) => (
              <Box key={item.text}>
                <ListItem
                  button
                  onClick={() => handleMenuClick(item)}
                  selected={isSelected(item.path)}
                  sx={{
                    borderRadius: "12px",
                    mb: 1,
                    minHeight: 48,
                    px: 2,
                    bgcolor: isSelected(item.path)
                      ? "linear-gradient(135deg, rgba(45, 91, 229, 0.08) 0%, rgba(45, 91, 229, 0.12) 100%)"
                      : "transparent",
                    color: isSelected(item.path) ? "#2d5be5ff" : "#4B5563",
                    border: isSelected(item.path)
                      ? "1px solid rgba(45, 91, 229, 0.2)"
                      : "1px solid transparent",
                    boxShadow: isSelected(item.path)
                      ? "0 2px 8px rgba(45, 91, 229, 0.08)"
                      : "none",
                    position: "relative",
                    overflow: "hidden",
                    "&::before": isSelected(item.path)
                      ? {
                          content: '""',
                          position: "absolute",
                          left: 0,
                          top: 0,
                          bottom: 0,
                          width: "3px",
                          bgcolor: "#2d5be5ff",
                          borderRadius: "0 2px 2px 0",
                        }
                      : {},
                    "&:hover": {
                      bgcolor: isSelected(item.path)
                        ? "linear-gradient(135deg, rgba(45, 91, 229, 0.12) 0%, rgba(45, 91, 229, 0.16) 100%)"
                        : "rgba(45, 91, 229, 0.04)",
                      color: isSelected(item.path) ? "#2d5be5ff" : "#2d5be5ff",
                      transform: "translateX(2px)",
                      boxShadow: "0 4px 12px rgba(45, 91, 229, 0.12)",
                    },
                    transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
                    "& .MuiListItemIcon-root": {
                      color: isSelected(item.path) ? "#2d5be5ff" : "#6B7280",
                      transition: "color 0.3s ease",
                    },
                    "&:hover .MuiListItemIcon-root": {
                      color: "#2d5be5ff",
                    },
                  }}
                >
                  <ListItemIcon
                    sx={{
                      minWidth: 40,
                    }}
                  >
                    {item.badge ? (
                      <Badge
                        badgeContent={item.badge}
                        sx={{
                          "& .MuiBadge-badge": {
                            bgcolor: "#EF4444",
                            color: "#ffffff",
                            fontSize: "0.7rem",
                            minWidth: "18px",
                            height: "18px",
                            fontWeight: 600,
                            boxShadow: "0 2px 4px rgba(239, 68, 68, 0.3)",
                          },
                        }}
                      >
                        {item.icon}
                      </Badge>
                    ) : (
                      item.icon
                    )}
                  </ListItemIcon>
                  <ListItemText
                    primary={item.text}
                    primaryTypographyProps={{
                      fontWeight: isSelected(item.path) ? 600 : 500,
                      fontSize: "0.9rem",
                      letterSpacing: "-0.01em",
                    }}
                  />
                  {item.subItems && (
                    <Box
                      sx={{
                        bgcolor: isSelected(item.path)
                          ? "rgba(45, 91, 229, 0.1)"
                          : "rgba(107, 114, 128, 0.08)",
                        borderRadius: "8px",
                        p: 0.5,
                        ml: 1,
                        transition: "all 0.2s ease",
                        "& svg": {
                          fontSize: "1rem",
                          color: isSelected(item.path)
                            ? "#2d5be5ff"
                            : "#6B7280",
                        },
                      }}
                    >
                      {item.text === "New Meeting" ? (
                        meetingMenuOpen ? (
                          <ExpandLess />
                        ) : (
                          <ExpandMore />
                        )
                      ) : item.text === "Recordings" ? (
                        recordingsMenuOpen ? (
                          <ExpandLess />
                        ) : (
                          <ExpandMore />
                        )
                      ) : null}
                    </Box>
                  )}
                </ListItem>

                {/* Submenu - UPDATED TO HANDLE BOTH NEW MEETING AND RECORDINGS */}
                {item.subItems &&
                  (item.text === "New Meeting" ||
                    item.text === "Recordings") && (
                    <Collapse
                      in={
                        item.text === "New Meeting"
                          ? meetingMenuOpen
                          : recordingsMenuOpen
                      }
                      timeout="auto"
                      unmountOnExit
                    >
                      <List
                        component="div"
                        disablePadding
                        sx={{ ml: 1, position: "relative" }}
                      >
                        {/* Connecting line */}
                        <Box
                          sx={{
                            position: "absolute",
                            left: "20px",
                            top: 0,
                            bottom: 0,
                            width: "1px",
                            bgcolor: "rgba(45, 91, 229, 0.15)",
                          }}
                        />
                        {item.subItems.map((subItem, index) => (
                          <ListItem
                            key={subItem.text}
                            button
                            onClick={() => handleSubMenuClick(subItem)}
                            selected={isSelected(subItem.path)}
                            sx={{
                              pl: 3,
                              borderRadius: "10px",
                              mb: 0.5,
                              minHeight: 40,
                              bgcolor: isSelected(subItem.path)
                                ? "rgba(45, 91, 229, 0.08)"
                                : "transparent",
                              color: isSelected(subItem.path)
                                ? "#2d5be5ff"
                                : "#6B7280",
                              border: isSelected(subItem.path)
                                ? "1px solid rgba(45, 91, 229, 0.2)"
                                : "1px solid transparent",
                              position: "relative",
                              "&::before": {
                                content: '""',
                                position: "absolute",
                                left: "20px",
                                top: "50%",
                                transform: "translateY(-50%)",
                                width: "8px",
                                height: "1px",
                                bgcolor: isSelected(subItem.path)
                                  ? "#2d5be5ff"
                                  : "rgba(45, 91, 229, 0.3)",
                              },
                              "&:hover": {
                                bgcolor: isSelected(subItem.path)
                                  ? "rgba(45, 91, 229, 0.12)"
                                  : "rgba(45, 91, 229, 0.04)",
                                color: "#2d5be5ff",
                                transform: "translateX(2px)",
                                "& .MuiListItemIcon-root": {
                                  color: "#2d5be5ff",
                                },
                              },
                              transition:
                                "all 0.2s cubic-bezier(0.4, 0, 0.2, 1)",
                              "& .MuiListItemIcon-root": {
                                color: isSelected(subItem.path)
                                  ? "#2d5be5ff"
                                  : "#9CA3AF",
                                transition: "color 0.2s ease",
                              },
                            }}
                          >
                            <ListItemIcon
                              sx={{
                                minWidth: 32,
                              }}
                            >
                              {subItem.icon}
                            </ListItemIcon>
                            <ListItemText
                              primary={subItem.text}
                              primaryTypographyProps={{
                                fontSize: "0.85rem",
                                fontWeight: isSelected(subItem.path)
                                  ? 600
                                  : 400,
                                letterSpacing: "-0.01em",
                              }}
                            />
                          </ListItem>
                        ))}
                      </List>
                    </Collapse>
                  )}
              </Box>
            ))}
          </List>
        </Box>

        {/* Professional Divider */}
        <Box
          sx={{
            mx: 2,
            height: "1px",
            background:
              "linear-gradient(90deg, transparent 0%, rgba(0,0,0,0.08) 50%, transparent 100%)",
            my: 2,
            flexShrink: 0,
          }}
        />

        {/* Bottom Menu - Fixed */}
        <Box
          sx={{
            px: 2,
            py: 1,
            flexShrink: 0,
          }}
        >
          <List sx={{ py: 0 }}>
            {bottomItems.map((item) => (
              <ListItem
                key={item.text}
                button
                onClick={() => {
                  navigate(item.path);
                  if (isMobile) setMobileOpen(false);
                }}
                selected={isSelected(item.path)}
                sx={{
                  borderRadius: "12px",
                  mb: 1,
                  minHeight: 48,
                  px: 2,
                  bgcolor: isSelected(item.path)
                    ? "linear-gradient(135deg, rgba(45, 91, 229, 0.08) 0%, rgba(45, 91, 229, 0.12) 100%)"
                    : "transparent",
                  color: isSelected(item.path) ? "#2d5be5ff" : "#4B5563",
                  border: isSelected(item.path)
                    ? "1px solid rgba(45, 91, 229, 0.2)"
                    : "1px solid transparent",
                  boxShadow: isSelected(item.path)
                    ? "0 2px 8px rgba(45, 91, 229, 0.08)"
                    : "none",
                  position: "relative",
                  "&::before": isSelected(item.path)
                    ? {
                        content: '""',
                        position: "absolute",
                        left: 0,
                        top: 0,
                        bottom: 0,
                        width: "3px",
                        bgcolor: "#2d5be5ff",
                        borderRadius: "0 2px 2px 0",
                      }
                    : {},
                  "&:hover": {
                    bgcolor: isSelected(item.path)
                      ? "linear-gradient(135deg, rgba(45, 91, 229, 0.12) 0%, rgba(45, 91, 229, 0.16) 100%)"
                      : "rgba(45, 91, 229, 0.04)",
                    color: "#2d5be5ff",
                    transform: "translateX(2px)",
                    boxShadow: "0 4px 12px rgba(45, 91, 229, 0.12)",
                    "& .MuiListItemIcon-root": {
                      color: "#2d5be5ff",
                    },
                  },
                  transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
                  "& .MuiListItemIcon-root": {
                    color: isSelected(item.path) ? "#2d5be5ff" : "#6B7280",
                    transition: "color 0.3s ease",
                  },
                }}
              >
                <ListItemIcon
                  sx={{
                    minWidth: 40,
                  }}
                >
                  {item.icon}
                </ListItemIcon>
                <ListItemText
                  primary={item.text}
                  primaryTypographyProps={{
                    fontWeight: isSelected(item.path) ? 600 : 500,
                    fontSize: "0.9rem",
                    letterSpacing: "-0.01em",
                  }}
                />
              </ListItem>
            ))}
          </List>
        </Box>
      </Box>
    </Box>
  );

  return (
    <Box sx={{ display: "flex", minHeight: "100vh" }}>
      {/* Mobile Menu Button - Enhanced */}
      {isMobile && !mobileOpen && (
        <IconButton
          color="inherit"
          edge="start"
          onClick={handleDrawerToggle}
          sx={{
            position: "fixed",
            top: 16,
            left: 16,
            zIndex: 1300,
            bgcolor: "#ffffff",
            color: "#3c6af6ff",
            border: "1px solid rgba(45, 91, 229, 0.2)",
            boxShadow: "0 4px 16px rgba(45, 91, 229, 0.15)",
            width: 48,
            height: 48,
            "&:hover": {
              bgcolor: "#3c6af6ff",
              color: "#ffffff",
              transform: "scale(1.05)",
              boxShadow: "0 6px 20px rgba(45, 91, 229, 0.25)",
            },
            transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
          }}
        >
          <MenuIcon />
        </IconButton>
      )}

      {/* Desktop Drawer */}
      {!isMobile && (
        <Drawer
          variant="permanent"
          sx={{
            width: drawerWidth,
            flexShrink: 0,
            [`& .MuiDrawer-paper`]: {
              width: drawerWidth,
              boxSizing: "border-box",
              border: "none",
              boxShadow:
                "4px 0 24px rgba(0,0,0,0.06), 0 0 0 1px rgba(0,0,0,0.04)",
              overflow: "hidden",
            },
          }}
        >
          {drawer}
        </Drawer>
      )}

      {/* Mobile Drawer */}
      {isMobile && (
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={handleDrawerToggle}
          ModalProps={{ keepMounted: true }}
          sx={{
            [`& .MuiDrawer-paper`]: {
              width: drawerWidth,
              boxSizing: "border-box",
              border: "none",
              boxShadow: "8px 0 32px rgba(0,0,0,0.12)",
            },
          }}
        >
          {drawer}
        </Drawer>
      )}

      {/* Main Content */}
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: 3,
          bgcolor: "#FAFBFC",
          minHeight: "100vh",
          ml: isMobile ? 0 : 0,
          pt: isMobile ? 8 : 3,
        }}
      >
        {children}
      </Box>
    </Box>
  );
};

export default DashboardLayout;
