// src/components/common/PlaceholderImage.jsx
import React from 'react';

const PlaceholderImage = ({ 
  width = 120, 
  height = 120, 
  name = "User", 
  className = "",
  showInitials = true,
  bgColor = "from-blue-500 to-purple-600",
  textColor = "text-white"
}) => {
  // Generate initials from name
  const getInitials = (name) => {
    if (!name) return "?";
    return name
      .split(' ')
      .filter(word => word.length > 0)
      .map(word => word[0])
      .join('')
      .toUpperCase()
      .slice(0, 2);
  };

  const initials = getInitials(name);
  
  return (
    <div 
      className={`
        flex items-center justify-center 
        bg-gradient-to-br ${bgColor} 
        ${textColor} font-semibold rounded-full
        ${className}
      `}
      style={{ 
        width: `${width}px`, 
        height: `${height}px`,
        fontSize: `${Math.max(width, height) * 0.3}px`
      }}
      title={name}
    >
      {showInitials ? initials : ""}
    </div>
  );
};

// Alternative component for meeting thumbnails
export const MeetingThumbnail = ({ 
  meetingName = "Meeting", 
  width = 48, 
  height = 48, 
  className = "" 
}) => {
  const icon = meetingName.includes('video') || meetingName.includes('call') ? '📹' : '👥';
  
  return (
    <div 
      className={`
        flex items-center justify-center 
        bg-gradient-to-br from-indigo-500 to-blue-600 
        text-white rounded-lg shadow-sm
        ${className}
      `}
      style={{ 
        width: `${width}px`, 
        height: `${height}px`,
        fontSize: `${Math.max(width, height) * 0.5}px`
      }}
      title={meetingName}
    >
      {icon}
    </div>
  );
};

// Component for user avatars with status
export const UserAvatar = ({ 
  user, 
  size = 'md', 
  showStatus = false, 
  isOnline = false,
  className = "" 
}) => {
  const sizeMap = {
    xs: { width: 24, height: 24 },
    sm: { width: 32, height: 32 },
    md: { width: 48, height: 48 },
    lg: { width: 64, height: 64 },
    xl: { width: 96, height: 96 }
  };
  
  const { width, height } = sizeMap[size] || sizeMap.md;
  
  return (
    <div className={`relative ${className}`}>
      <PlaceholderImage
        width={width}
        height={height}
        name={user?.name || user?.full_name || user?.email || "User"}
        className="border-2 border-white shadow-sm"
      />
      {showStatus && (
        <div 
          className={`
            absolute -bottom-1 -right-1 w-3 h-3 rounded-full border-2 border-white
            ${isOnline ? 'bg-green-500' : 'bg-gray-400'}
          `}
        />
      )}
    </div>
  );
};

export default PlaceholderImage;