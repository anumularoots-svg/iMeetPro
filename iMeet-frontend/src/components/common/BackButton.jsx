// src/components/common/BackButton.jsx
import React from 'react';
import { useNavigate } from 'react-router-dom';
import { IconButton, Button, Tooltip } from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';

const BackButton = ({ 
  variant = 'icon', // 'icon' | 'text' | 'contained' | 'outlined'
  size = 'medium',
  color = 'primary',
  showTooltip = true,
  tooltipText = 'Go back',
  className = '',
  disabled = false,
  onClick = null, // Custom onClick handler (optional)
  ...props 
}) => {
  const navigate = useNavigate();

  const handleClick = () => {
    if (onClick) {
      onClick();
    } else {
      navigate(-1);
    }
  };

  // Icon button variant
  if (variant === 'icon') {
    const iconButton = (
      <IconButton
        onClick={handleClick}
        size={size}
        color={color}
        disabled={disabled}
        className={className}
        {...props}
      >
        <ArrowBackIcon />
      </IconButton>
    );

    return showTooltip ? (
      <Tooltip title={tooltipText} arrow>
        {iconButton}
      </Tooltip>
    ) : iconButton;
  }

  // Text/Button variants
  return (
    <Button
      variant={variant}
      startIcon={<ArrowBackIcon />}
      onClick={handleClick}
      size={size}
      color={color}
      disabled={disabled}
      className={className}
      {...props}
    >
      Back
    </Button>
  );
};

export default BackButton;