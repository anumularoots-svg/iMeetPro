// Enhanced Sidebar.jsx - FIXED VERSION WITH DEBUGGING
import React, { useState } from "react";
import {
  Drawer,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Divider,
  Typography,
  Box,
  Collapse,
  IconButton,
  Badge,
  Tooltip,
  useTheme,
  useMediaQuery,
  Avatar,
} from "@mui/material";
import {
  Dashboard,
  VideoCall,
  Schedule,
  CalendarMonth,
  VideoLibrary,
  Analytics,
  Settings,
  Person,
  ExpandLess,
  ExpandMore,
  Add,
  ChevronLeft,
  ChevronRight,
  AccessTime,
  Event,
  PlayCircle,
  Notifications,
} from "@mui/icons-material";
import { useNavigate, useLocation } from "react-router-dom";

const Sidebar = ({ open, onClose, variant = "temporary" }) => {
  const [meetingSubmenuOpen, setMeetingSubmenuOpen] = useState(false);
  const [recordingsSubmenuOpen, setRecordingsSubmenuOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down("md"));

  // FIXED: Recordings item with explicit submenu structure
  const recordingsMenuItem = {
    text: "Recordings",
    icon: <VideoLibrary />,
    path: "/recordings",
    badge: 5,
    hasSubmenu: true, // CRITICAL
    submenu: [
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
  };

  const mainMenuItems = [
    {
      text: "Dashboard",
      icon: <Dashboard />,
      path: "/dashboard",
      badge: null,
      hasSubmenu: false,
    },
    {
      text: "New Meeting",
      icon: <VideoCall />,
      path: "/meeting/new",
      badge: null,
      hasSubmenu: true,
      submenu: [
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
      badge: 3,
      hasSubmenu: false,
    },
    {
      text: "Calendar",
      icon: <CalendarMonth />,
      path: "/calendar",
      badge: null,
      hasSubmenu: false,
    },
    recordingsMenuItem, // Using the explicitly defined object
    {
      text: "Analytics",
      icon: <Analytics />,
      path: "/analytics",
      badge: null,
      hasSubmenu: false,
    },
  ];

  const bottomMenuItems = [
    { text: "Profile", icon: <Person />, path: "/profile" },
    { text: "Settings", icon: <Settings />, path: "/settings" },
  ];

  const handleItemClick = (item) => {
    console.log("🔘 CLICKED:", item.text);
    console.log("🔘 hasSubmenu:", item.hasSubmenu);
    console.log("🔘 submenu array:", item.submenu);
    
    if (item.hasSubmenu === true) {
      if (item.text === "New Meeting") {
        console.log("🔘 Toggling New Meeting submenu");
        setMeetingSubmenuOpen(!meetingSubmenuOpen);
      } else if (item.text === "Recordings") {
        console.log("🔘 Toggling Recordings submenu");
        setRecordingsSubmenuOpen(!recordingsSubmenuOpen);
      }
    } else {
      console.log("🔘 Navigating to:", item.path);
      navigate(item.path);
      if (isMobile && onClose) {
        onClose();
      }
    }
  };

  const handleSubmenuClick = (item) => {
    console.log("🔘 Submenu clicked:", item.text, "->", item.path);
    navigate(item.path);
    if (isMobile && onClose) {
      onClose();
    }
    setMeetingSubmenuOpen(false);
    setRecordingsSubmenuOpen(false);
  };

  const isSelected = (path) => location.pathname === path;

  const drawerWidth = collapsed ? 80 : 280;

  const drawerContent = (
    <Box
      sx={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        background:
          "linear-gradient(180deg, #4F46E5 0%, #7C3AED 50%, #EC4899 100%)",
        position: "relative",
        overflow: "hidden",
      }}
    >
      {/* Decorative overlay */}
      <Box
        sx={{
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: "rgba(255, 255, 255, 0.05)",
          backdropFilter: "blur(10px)",
          zIndex: 0,
        }}
      />

      {/* Main Menu */}
      <List
        sx={{
          flexGrow: 1,
          px: collapsed ? 1.5 : 2,
          py: 1,
          position: "relative",
          zIndex: 1,
        }}
      >
        {mainMenuItems.map((item, index) => {
          // Debug log for each item
          if (item.text === "Recordings") {
            console.log("🔍 Rendering Recordings item:", {
              hasSubmenu: item.hasSubmenu,
              submenuLength: item.submenu?.length,
            });
          }

          return (
            <Box key={item.text}>
              <Tooltip title={collapsed ? item.text : ""} placement="right">
                <ListItem
                  button
                  onClick={() => handleItemClick(item)}
                  selected={isSelected(item.path)}
                  sx={{
                    borderRadius: "12px",
                    mb: 1,
                    minHeight: 48,
                    justifyContent: collapsed ? "center" : "flex-start",
                    px: collapsed ? 1 : 2,
                    bgcolor: isSelected(item.path)
                      ? "rgba(255, 255, 255, 0.25)"
                      : "transparent",
                    color: "white",
                    backdropFilter: isSelected(item.path) ? "blur(10px)" : "none",
                    border: isSelected(item.path)
                      ? "1px solid rgba(255, 255, 255, 0.3)"
                      : "1px solid transparent",
                    "&:hover": {
                      bgcolor: "rgba(255, 255, 255, 0.2)",
                      backdropFilter: "blur(10px)",
                      transform: "translateX(4px)",
                      border: "1px solid rgba(255, 255, 255, 0.3)",
                    },
                    transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
                  }}
                >
                  <ListItemIcon
                    sx={{
                      color: "white",
                      minWidth: collapsed ? "auto" : 40,
                      justifyContent: "center",
                    }}
                  >
                    {item.badge && !collapsed && !item.hasSubmenu ? (
                      <Badge
                        badgeContent={item.badge}
                        sx={{
                          "& .MuiBadge-badge": {
                            bgcolor: "#EF4444",
                            color: "white",
                            fontSize: "0.7rem",
                            minWidth: "18px",
                            height: "18px",
                          },
                        }}
                      >
                        {item.icon}
                      </Badge>
                    ) : (
                      item.icon
                    )}
                  </ListItemIcon>

                  {!collapsed && (
                    <>
                      <ListItemText
                        primary={item.text}
                        primaryTypographyProps={{
                          fontWeight: 500,
                          fontSize: "0.9rem",
                        }}
                      />
                      
                      {/* Show badge only if no submenu */}
                      {item.badge && !item.hasSubmenu && (
                        <Badge
                          badgeContent={item.badge}
                          sx={{
                            "& .MuiBadge-badge": {
                              bgcolor: "#EF4444",
                              color: "white",
                              fontSize: "0.7rem",
                              minWidth: "18px",
                              height: "18px",
                            },
                          }}
                        />
                      )}
                      
                      {/* CRITICAL: Show dropdown arrow if hasSubmenu is true */}
                      {item.hasSubmenu === true && (
                        <Box
                          sx={{
                            bgcolor: "rgba(255, 255, 255, 0.2)",
                            borderRadius: "6px",
                            p: 0.5,
                            ml: 1,
                            display: "flex",
                            alignItems: "center",
                          }}
                        >
                          {(item.text === "New Meeting" && meetingSubmenuOpen) ||
                          (item.text === "Recordings" && recordingsSubmenuOpen) ? (
                            <ExpandLess fontSize="small" />
                          ) : (
                            <ExpandMore fontSize="small" />
                          )}
                        </Box>
                      )}
                    </>
                  )}
                </ListItem>
              </Tooltip>

              {/* Submenu - CRITICAL: Check hasSubmenu === true */}
              {item.hasSubmenu === true && !collapsed && item.submenu && (
                <Collapse
                  in={
                    item.text === "New Meeting"
                      ? meetingSubmenuOpen
                      : item.text === "Recordings"
                      ? recordingsSubmenuOpen
                      : false
                  }
                  timeout="auto"
                  unmountOnExit
                >
                  <List component="div" disablePadding sx={{ ml: 2 }}>
                    {item.submenu.map((subItem) => (
                      <Tooltip
                        key={subItem.text}
                        title={collapsed ? subItem.text : ""}
                        placement="right"
                      >
                        <ListItem
                          button
                          onClick={() => handleSubmenuClick(subItem)}
                          selected={isSelected(subItem.path)}
                          sx={{
                            pl: 3,
                            borderRadius: "10px",
                            mb: 0.5,
                            minHeight: 40,
                            bgcolor: isSelected(subItem.path)
                              ? "rgba(255, 255, 255, 0.2)"
                              : "transparent",
                            color: "white",
                            border: "1px solid rgba(255, 255, 255, 0.1)",
                            "&:hover": {
                              bgcolor: "rgba(255, 255, 255, 0.15)",
                              transform: "translateX(4px)",
                              border: "1px solid rgba(255, 255, 255, 0.3)",
                            },
                            transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
                          }}
                        >
                          <ListItemIcon
                            sx={{
                              minWidth: 32,
                              color: "white",
                              "& svg": { fontSize: "1.1rem" },
                            }}
                          >
                            {subItem.icon}
                          </ListItemIcon>
                          <ListItemText
                            primary={subItem.text}
                            primaryTypographyProps={{
                              fontSize: "0.85rem",
                              fontWeight: 400,
                            }}
                          />
                        </ListItem>
                      </Tooltip>
                    ))}
                  </List>
                </Collapse>
              )}
            </Box>
          );
        })}
      </List>

      {/* Divider */}
      <Box
        sx={{
          mx: collapsed ? 1.5 : 2,
          height: "1px",
          background:
            "linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent)",
          my: 1,
        }}
      />

      {/* Bottom Menu */}
      <List
        sx={{
          px: collapsed ? 1.5 : 2,
          py: 1,
          position: "relative",
          zIndex: 1,
        }}
      >
        {bottomMenuItems.map((item) => (
          <Tooltip
            key={item.text}
            title={collapsed ? item.text : ""}
            placement="right"
          >
            <ListItem
              button
              onClick={() => {
                navigate(item.path);
                if (isMobile && onClose) onClose();
              }}
              selected={isSelected(item.path)}
              sx={{
                borderRadius: "12px",
                mb: 1,
                minHeight: 48,
                justifyContent: collapsed ? "center" : "flex-start",
                px: collapsed ? 1 : 2,
                color: "white",
                bgcolor: isSelected(item.path)
                  ? "rgba(255, 255, 255, 0.25)"
                  : "transparent",
                backdropFilter: isSelected(item.path) ? "blur(10px)" : "none",
                border: isSelected(item.path)
                  ? "1px solid rgba(255, 255, 255, 0.3)"
                  : "1px solid transparent",
                "&:hover": {
                  bgcolor: "rgba(255, 255, 255, 0.2)",
                  backdropFilter: "blur(10px)",
                  transform: "translateX(4px)",
                  border: "1px solid rgba(255, 255, 255, 0.3)",
                },
                transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
              }}
            >
              <ListItemIcon
                sx={{
                  color: "white",
                  minWidth: collapsed ? "auto" : 40,
                  justifyContent: "center",
                }}
              >
                {item.icon}
              </ListItemIcon>
              {!collapsed && (
                <ListItemText
                  primary={item.text}
                  primaryTypographyProps={{
                    fontWeight: 500,
                    fontSize: "0.9rem",
                  }}
                />
              )}
            </ListItem>
          </Tooltip>
        ))}
      </List>
    </Box>
  );

  if (variant === "permanent") {
    return (
      <Drawer
        variant="permanent"
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          [`& .MuiDrawer-paper`]: {
            width: drawerWidth,
            boxSizing: "border-box",
            border: "none",
            boxShadow: "8px 0 32px rgba(0,0,0,0.12)",
            transition: "width 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
            overflow: "hidden",
          },
        }}
      >
        {drawerContent}
      </Drawer>
    );
  }

  return (
    <Drawer
      anchor="left"
      open={open}
      onClose={onClose}
      variant={variant}
      ModalProps={{ keepMounted: true }}
      sx={{
        [`& .MuiDrawer-paper`]: {
          width: 280,
          boxSizing: "border-box",
          border: "none",
          boxShadow: "8px 0 32px rgba(0,0,0,0.12)",
        },
      }}
    >
      {drawerContent}
    </Drawer>
  );
};

export default Sidebar;